import time
from threading import Lock
from typing import Any, NoReturn

from absl import app, logging
from influxdb_client import Point
from influxdb_client.client.influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS, WriteApi

from line_protocol_cache.consumer.consumer import Consumer
from line_protocol_cache.consumer.flag import (BUCKET, BUCKET_ORG, BUCKET_TOKEN, DERIVED_BUCKET,
                                               DERIVED_BUCKET_ORG, DERIVED_BUCKET_TOKEN,
                                               DERIVED_SERVER_URL, HTTP_TIMEOUT_MILLISECONDS,
                                               SERVER_URL)


def _client() -> InfluxDBClient:
  return InfluxDBClient(
      url=str(SERVER_URL.value),
      token=str(BUCKET_TOKEN.value),
      org=str(BUCKET_ORG.value),
      timeout=int(HTTP_TIMEOUT_MILLISECONDS.value),
  )


def _derived_client() -> InfluxDBClient | Any:
  if DERIVED_BUCKET.value is None:
    # Using Lock to provide a placeholder object that has __enter__() and __exit__().
    return Lock()

  return InfluxDBClient(
      url=str(DERIVED_SERVER_URL.value),
      token=str(DERIVED_BUCKET_TOKEN.value),
      org=str(DERIVED_BUCKET_ORG.value),
      timeout=int(HTTP_TIMEOUT_MILLISECONDS.value),
  )


def _consume_and_write(consumer: Consumer, write_api: WriteApi,
                       derived_write_api: WriteApi | None) -> None:
  if derived_write_api is not None and (max_rowid := consumer.max_rowid()) is not None:
    point = Point.measurement('line_protocol_cache')
    point.field('max_rowid', max_rowid)
    point.time(time.time_ns())  # type: ignore
    derived_write_api.write(bucket=str(DERIVED_BUCKET.value), record=point)

  line_protocols = consumer.get()
  if len(line_protocols) > 0:
    write_api.write(bucket=str(BUCKET.value), record=list(line_protocols.values()))
    consumer.delete(rowids=list(line_protocols.keys()))


def main(args: list[str]) -> NoReturn:
  with Consumer() as consumer, _client() as client, _derived_client() as derived_client:
    write_api = client.write_api(write_options=SYNCHRONOUS)
    logging.info('Connected to Write API.')

    derived_write_api = None
    if isinstance(derived_client, InfluxDBClient):
      derived_write_api = derived_client.write_api(write_options=SYNCHRONOUS)
      logging.info('Connected to Derived Write API.')
    else:
      logging.info('Did not connect to Derived Write API.')

    while True:
      is_catching_up = (max_rowid := consumer.max_rowid()) is not None and max_rowid >= 5000
      time.sleep(0.5 if is_catching_up else 5)

      _consume_and_write(consumer, write_api, derived_write_api)


def app_run_main() -> None:
  app.run(main)
