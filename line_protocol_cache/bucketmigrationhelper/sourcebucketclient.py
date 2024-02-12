from typing import Self

from absl import flags, logging
from influxdb_client import InfluxDBClient
from influxdb_client.client.flux_table import FluxRecord, TableList
from tenacity import Retrying, after_log, stop_after_attempt, wait_fixed

from line_protocol_cache.bucketmigrationhelper.timestamp import Timestamp

_SRC_SERVER_URL = flags.DEFINE_string(
    name='src_server_url',
    default=None,
    required=True,
    help='InfluxDB server API URL for the source bucket (ex. http://localhost:8086).',
)
_SRC_BUCKET = flags.DEFINE_string(
    name='src_bucket',
    default=None,
    required=True,
    help='Specifies the source bucket for reading the metrics from.',
)
_SRC_TOKEN = flags.DEFINE_string(
    name='src_token',
    default=None,
    required=True,
    help='Token to authenticate to read the source bucket.',
)
_SRC_ORG = flags.DEFINE_string(
    name='src_org',
    default=None,
    required=True,
    help='Organization name of the source bucket.',
)

_MAX_ATTEMPTS = flags.DEFINE_integer(
    name='max_attempts',
    default=6,
    help='Max times to send the query before giving up.',
)
_RETRY_INTERVAL_S = flags.DEFINE_float(
    name='retry_interval_s',
    default=10.0,
    help='Time in seconds to wait between resending the query.',
)
_TIMEOUT_MS = flags.DEFINE_integer(
    name='timeout_ms',
    default=60 * 1000,
    help='',
)


class SourceBucketClient:

  def __enter__(self) -> Self:
    self._client = InfluxDBClient(
        _SRC_SERVER_URL.value,
        _SRC_TOKEN.value,
        org=_SRC_ORG.value,
        timeout=(_TIMEOUT_MS.value if _TIMEOUT_MS.present else _TIMEOUT_MS.default),
    )
    self._query_api = self._client.query_api()
    self._retrying = Retrying(
        stop=stop_after_attempt(_MAX_ATTEMPTS.value if _MAX_ATTEMPTS.present else _MAX_ATTEMPTS.default),
        wait=wait_fixed(_RETRY_INTERVAL_S.value if _RETRY_INTERVAL_S.present else _RETRY_INTERVAL_S.default),
        after=after_log(logger=logging, log_level=logging.WARNING),  # type: ignore
        reraise=True,
    )
    return self

  def __exit__(self, exception_type, exception_value, exception_traceback) -> None:
    self._client.close()

  @staticmethod
  def only_int_value(tables: TableList, default_value: int) -> int:
    if len(tables) == 0:
      return default_value

    assert len(tables) == 1
    assert len(records := tables[0].records) == 1
    assert isinstance(record := records[0], FluxRecord)
    assert isinstance(value := record.get_value(), int)

    return value

  def execute(
      self,
      range_start: Timestamp,
      range_stop: Timestamp,
      query_lines: list[str | None] | list[str] = [],
  ) -> TableList:
    query = ' |> '.join([
        f'from(bucket: "{_SRC_BUCKET.value}")',
        f'range(start: {range_start}, stop: {range_stop})',
        *[q for q in query_lines if q is not None],
        'yield()',
    ])
    logging.debug(query)

    return self._retrying(self._query_api.query, query)

  def assert_non_empty_bucket(self) -> None:
    query_lines = [
        'first()',
        'map(fn: (r) => ({_time: r._time, _field: "field", _value: 1}))',
        'count()',
    ]

    tables = self.execute(Timestamp.min(), Timestamp.max(), query_lines)
    if len(tables) == 0:
      raise ValueError('Bucket seems to be empty. Cannot continue with an empty bucket.')

  def min_timestamp(self) -> Timestamp:
    logging.info(f'Querying source bucket for min timestamp.')
    query_lines = [
        'first()',
        'map(fn: (r) => ({_value: int(v: r._time)}))',
        'min()',
    ]

    tables = self.execute(Timestamp.min(), Timestamp.max(), query_lines)
    timestamp = Timestamp(self.only_int_value(tables, Timestamp.min().nanoseconds))

    logging.debug(f'Min timestamp is {timestamp}, {timestamp.nanoseconds}.')
    return timestamp

  def max_timestamp(self) -> Timestamp:
    logging.info(f'Querying source bucket for max timestamp + 1.')
    query_lines = [
        'last()',
        'map(fn: (r) => ({_value: int(v: r._time)}))',
        'max()',
    ]

    tables = self.execute(Timestamp.min(), Timestamp.max(), query_lines)
    timestamp = Timestamp(self.only_int_value(tables, Timestamp.max().nanoseconds - 1))
    timestamp += 1  # Include the row with the timestamp. See: https://docs.influxdata.com/flux/v0/stdlib/universe/range/#stop.

    logging.debug(f'Max timestamp + 1 is {timestamp}, {timestamp.nanoseconds}.')
    return timestamp
