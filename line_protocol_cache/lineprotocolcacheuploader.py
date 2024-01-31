import math
import os
import signal
import sqlite3
from sys import float_info
from threading import Event
from typing import Self

from absl import app, flags, logging
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

_SERVER_URL = flags.DEFINE_string(
    name='server_url',
    default=None,
    required=True,
    help='InfluxDB server API URL (ex. http://localhost:8086).',
)
_BUCKET = flags.DEFINE_string(
    name='bucket',
    default=None,
    required=True,
    help='Specifies the destination bucket for collected metrics.',
)
_BUCKET_TOKEN = flags.DEFINE_string(
    name='bucket_token',
    default=None,
    required=True,
    help='Token to authenticate to the InfluxDB 2.x.',
)
_BUCKET_ORG = flags.DEFINE_string(
    name='bucket_org',
    default=None,
    required=True,
    help='Organization name.',
)
_HTTP_TIMEOUT = flags.DEFINE_integer(
    name='http_timeout_ms',
    default=10_000,
    help='HTTP client timeout setting for a request specified in milliseconds.',
)

_CACHE_PATH = flags.DEFINE_string(
    name='cache_path',
    default='data/line-protocol-cache/cache.sqlite',
    help='Relative or absolute path to the SQLite cache file. '
    'More files may be created under the same path.',
)
_SQLITE_TIMEOUT = flags.DEFINE_float(
    name='sqlite_timeout_s',
    default=20.0,
    lower_bound=float_info.min,
    help='SQLite timeout in seconds. Adjust this limit if the file system is lagging.',
)
_UPLOAD_INTERVAL = flags.DEFINE_float(
    name='upload_interval_s',
    default=5.0,
    lower_bound=float_info.min,
    help='Interval in seconds between uploads.',
)
_CATCHING_UP_INTERVAL = flags.DEFINE_float(
    name='catching_up_interval_s',
    default=0.5,
    lower_bound=float_info.min,
    help='Interval in seconds when there are backlogs in the cache file.',
)
_SAMPLE_INTERVAL = flags.DEFINE_float(
    name='sample_interval_s',
    default=math.inf,
    lower_bound=0.0,
    help='Time in seconds between selecting a lucky line protocol to be printed. '
    'Supply value 0 to print every line protocol.',
)
_BATCH_SIZE = flags.DEFINE_integer(
    name='batch_size',
    default=5000,
    lower_bound=1,
    help='Number of line protocols to upload at once. '
    'The optimal batch size is 5000 lines of line protocol. '
    'https://docs.influxdata.com/influxdb/v2.6/write-data/best-practices/optimize-writes/#batch-writes.',
)


class LineProtocolCacheUploader:
  _ENABLE_WAL = 'PRAGMA journal_mode=WAL;'  # https://www.sqlite.org/wal.html
  _CREATE_TABLE = 'CREATE TABLE IF NOT EXISTS LineProtocolCache (line_protocol TEXT NOT NULL);'
  _SELECT_ROWS = 'SELECT rowid, line_protocol FROM LineProtocolCache;'
  _DELETE_ROW = 'DELETE FROM LineProtocolCache WHERE rowid = ?;'
  _COUNT = 'SELECT COUNT(line_protocol) FROM LineProtocolCache;'

  def __enter__(self) -> Self:
    os.makedirs(os.path.dirname(_CACHE_PATH.value), exist_ok=True)

    self._connection = sqlite3.connect(database=_CACHE_PATH.value, timeout=_SQLITE_TIMEOUT.value)
    with self._connection:
      self._connection.execute(self._ENABLE_WAL)
      self._connection.execute(self._CREATE_TABLE)

    self._client = InfluxDBClient(
        url=_SERVER_URL.value,
        token=_BUCKET_TOKEN.value,
        org=_BUCKET_ORG.value,
        timeout=_HTTP_TIMEOUT.value,
    )
    self._write_api = self._client.write_api(write_options=SYNCHRONOUS)

    return self

  def __exit__(self, exception_type, exception_value, exception_traceback) -> None:
    self._client.close()
    self._connection.close()

  def _get_rows(self) -> dict[int, str]:
    with self._connection:
      raw_rows = self._connection.execute(self._SELECT_ROWS).fetchmany(_BATCH_SIZE.value)

    rows: dict[int, str] = dict()

    for raw_row in raw_rows:
      if (isinstance(raw_row, tuple) and len(raw_row) == 2 and isinstance(rowid := raw_row[0], int) and
          isinstance(row := raw_row[1], str)):
        rows[rowid] = row
        logging.log_every_n_seconds(logging.INFO, row, _SAMPLE_INTERVAL.value)
        continue

      e = ValueError('Invalid row. Check query and cache file.')
      e.add_note(f'{raw_row=}')
      e.add_note(f'{self._SELECT_ROWS=}')
      raise e

    return rows

  def _delete_rows(self, rowids: list[int]) -> None:
    with self._connection:
      self._connection.executemany(self._DELETE_ROW, [(rowid,) for rowid in rowids])

  def _upload_rows(self, rows: list[str]) -> None:
    if len(rows) != 0:
      self._write_api.write(bucket=_BUCKET.value, record=rows)

  def _get_count(self) -> int:
    with self._connection:
      rows = self._connection.execute(self._COUNT).fetchall()

    if (len(rows) == 1 and isinstance(row := rows[0], tuple) and len(row) == 1 and isinstance(count := row[0], int)):
      return count

    e = ValueError('Invalid row when querying count.')
    e.add_note(f'{rows=}')
    e.add_note(f'{self._COUNT=}')
    raise e

  def run(self, stop_running: Event = Event()) -> None:
    while not stop_running.is_set():
      count = self._get_count()
      if count > _BATCH_SIZE.value:
        logging.info(f'Catching up, {count=}.')
        stop_running.wait(_CATCHING_UP_INTERVAL.value)
      else:
        stop_running.wait(_UPLOAD_INTERVAL.value)

      rows = self._get_rows()
      self._upload_rows(list(rows.values()))
      self._delete_rows(list(rows.keys()))


def main(args: list[str]) -> None:
  with LineProtocolCacheUploader() as uploader:
    stop_running = Event()
    signal.signal(signal.SIGTERM, lambda signal_number, stack_frame: stop_running.set())

    uploader.run(stop_running)


def app_run_main() -> None:
  app.run(main)


if __name__ == '__main__':
  app_run_main()
