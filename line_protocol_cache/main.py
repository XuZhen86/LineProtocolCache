import time
from threading import Lock
from typing import Any, NoReturn

from absl import app, flags, logging
from influxdb_client import Point
from influxdb_client.client.influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import WriteApi, SYNCHRONOUS

from line_protocol_cache import defaults
from line_protocol_cache.consumer import LineProtocolCacheConsumer

_CACHE_PATH = flags.DEFINE_string(
    name='cache_path',
    default=defaults.CACHE_PATH,
    help='path to the SQLite cache file.',
)
_CACHE_TIMEOUT_SECONDS = flags.DEFINE_float(
    name='cache_timeout_seconds',
    default=defaults.TIMEOUT,
    help='SQLite connection timeout.',
)

_SERVER_URL = flags.DEFINE_string(
    name='server_url',
    default=None,
    required=True,
    help='InfluxDB server API url (ex. http://localhost:8086).',
)
_BUCKET = flags.DEFINE_string(
    name='bucket',
    default=None,
    required=True,
    help='specifies the destination bucket for collected metrics.',
)
_BUCKET_TOKEN = flags.DEFINE_string(
    name='bucket_token',
    default=None,
    required=True,
    help='token to authenticate to the InfluxDB 2.x.',
)
_BUCKET_ORG = flags.DEFINE_string(
    name='bucket_org',
    default=None,
    required=True,
    help='organization name.',
)

_DERIVED_SERVER_URL = flags.DEFINE_string(
    name='derived_server_url',
    default=None,
    help='InfluxDB server API url (ex. http://localhost:8086).',
)
_DERIVED_BUCKET = flags.DEFINE_string(
    name='derived_bucket',
    default=None,
    help='specifies the destination bucket for collected metrics.',
)
_DERIVED_BUCKET_TOKEN = flags.DEFINE_string(
    name='derived_bucket_token',
    default=None,
    help='token to authenticate to the InfluxDB 2.x.',
)
_DERIVED_BUCKET_ORG = flags.DEFINE_string(
    name='derived_bucket_org',
    default=None,
    help='organization name.',
)

_RECONNECT_INTERVAL_SECONDS = flags.DEFINE_integer(
    name='reconnect_interval_seconds',
    default=10,
    help='seconds to delay before attempting to reconnect.',
)


def _get_consumer() -> LineProtocolCacheConsumer:
  return LineProtocolCacheConsumer(
      cache_path=_CACHE_PATH.value,
      timeout=_CACHE_TIMEOUT_SECONDS.value,
  )


def _get_bucket_client() -> InfluxDBClient:
  return InfluxDBClient(
      url=_SERVER_URL.value,
      token=_BUCKET_TOKEN.value,
      org=_BUCKET_ORG.value,
  )


def _get_derived_bucket_client() -> InfluxDBClient | Any:
  if _DERIVED_BUCKET.value is None:
    # Using Lock to provide a placeholder object that has __enter__() and __exit__().
    return Lock()

  if _DERIVED_SERVER_URL.value is None:
    logging.warn('derived_server_url was not specified, defaulting to server_url.')
  if _DERIVED_BUCKET_TOKEN.value is None:
    logging.warn('derived_bucket_token was not specified, defaulting to bucket_token.')
  if _DERIVED_BUCKET_ORG.value is None:
    logging.warn('derived_bucket_org was not specified, defaulting to bucket_org.')

  return InfluxDBClient(
      url=(_DERIVED_SERVER_URL.value
           if _DERIVED_SERVER_URL.value is not None else _SERVER_URL.value),
      token=(_DERIVED_BUCKET_TOKEN.value
             if _DERIVED_BUCKET_TOKEN.value is not None else _BUCKET_TOKEN.value),
      org=(_DERIVED_BUCKET_ORG.value
           if _DERIVED_BUCKET_ORG.value is not None else _BUCKET_ORG.value),
  )


def _consume_and_write_line_protocols(
    consumer: LineProtocolCacheConsumer,
    bucket_write_api: WriteApi,
    derived_bucket_write_api: WriteApi | None,
) -> None:
  if derived_bucket_write_api is not None and (max_rowid := consumer.get_max_rowid()) is not None:
    point = Point.measurement('line_protocol_cache').field('max_rowid', max_rowid).time(
        time.time_ns())  # type: ignore
    derived_bucket_write_api.write(bucket=_DERIVED_BUCKET.value, record=point)

  line_protocols = consumer.get()
  if len(line_protocols) > 0:
    bucket_write_api.write(bucket=_BUCKET.value, record=list(line_protocols.values()))
    consumer.delete(rowids=list(line_protocols.keys()))


def run_line_protocol_cache_consumer(args: list[str]) -> NoReturn:
  with _get_consumer() as consumer, _get_bucket_client(
  ) as bucket_client, _get_derived_bucket_client() as derived_bucket_client:
    bucket_write_api = bucket_client.write_api(write_options=SYNCHRONOUS)

    derived_bucket_write_api = None
    if isinstance(derived_bucket_client, InfluxDBClient):
      derived_bucket_write_api = derived_bucket_client.write_api(write_options=SYNCHRONOUS)

    while True:
      is_catching_up = (max_rowid := consumer.get_max_rowid()) is not None and max_rowid >= 5000
      time.sleep(0.5 if is_catching_up else 5)

      _consume_and_write_line_protocols(
          consumer=consumer,
          bucket_write_api=bucket_write_api,
          derived_bucket_write_api=derived_bucket_write_api,
      )


def infinite_retry_wrapper(args: list[str]) -> NoReturn:
  while True:
    try:
      run_line_protocol_cache_consumer(args)
    except:
      time.sleep(_RECONNECT_INTERVAL_SECONDS.value)


def main() -> None:
  app.run(infinite_retry_wrapper)
