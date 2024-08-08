import json
from typing import Self

from absl import flags, logging
from influxdb_client import InfluxDBClient
from jsonschema import Draft202012Validator
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_fixed

from .bucketinfo import BucketInfo
from .timestamp import Timestamp
from .timestamprange import TimestampRange

_DRY_RUN = flags.DEFINE_bool(
    name='dry_run',
    default=True,
    help='Controls if the queried records are written to the target bucket. '
    'Note: This flag does not control if anything in "flux_query" would mutate the target bucket. '
    'Set this flag to False for the migration.',
)

_FLUX_QUERY = flags.DEFINE_multi_string(
    name='flux_query',
    default=[],
    help='Lines of Flux query to process the source data points before sending them to the target bucket. '
    'Use --verbosity=1 to preview the assembled queries.',
)


class BucketClient:

  def __init__(self, bucket_info: BucketInfo) -> None:
    self._DRY_RUN = _DRY_RUN.value if _DRY_RUN.present else _DRY_RUN.default
    self._FLUX_QUERY = _FLUX_QUERY.value if _FLUX_QUERY.present else _FLUX_QUERY.default

    self._bucket_info = bucket_info

  def __enter__(self) -> Self:
    self._client = InfluxDBClient(url=self._bucket_info.server_url,
                                  token=self._bucket_info.token,
                                  org=self._bucket_info.organization,
                                  timeout=60_000,
                                  enable_gzip=True)
    self._query_api = self._client.query_api()
    return self

  def __exit__(self, exception_type, exception_value, exception_traceback) -> None:
    self._client.close()

  @retry(
      before_sleep=before_sleep_log(logger=logging, log_level=logging.WARNING),  # type: ignore
      wait=wait_fixed(5),
      stop=stop_after_attempt(5),
      reraise=True)
  def execute_query(self, ts_range: TimestampRange, query_lines: list[str]) -> list[dict]:
    query = ' |> '.join([
        f'from(bucket: "{self._bucket_info.bucket}")',
        str(ts_range),
        *query_lines,
        'yield()',
    ])
    logging.debug(f'{query=}')

    tables = json.loads(self._query_api.query(query).to_json())
    _LIST_OF_DICTS_VALIDATOR.validate(tables)
    return tables

  def is_emtpy(self) -> bool:
    query_lines = [
        'last()',
        'map(fn: (r) => ({_value: int(v: r["_time"])}))',
        'count()',
    ]
    tables = self.execute_query(TimestampRange.ETERNITY, query_lines)
    return len(tables) == 0

  def get_min_timestamp(self) -> Timestamp:
    query_lines = [
        'first()',
        'map(fn: (r) => ({_value: int(v: r["_time"])}))',
        'min()',
    ]
    tables = self.execute_query(TimestampRange.ETERNITY, query_lines)
    _SINGLE_INT_VALUE_VALIDATOR.validate(tables)

    if len(tables) == 0:
      raise ValueError('no result was returned, is the bucket empty?')
    return Timestamp(int(tables[0]['_value']))

  def get_max_timestamp(self) -> Timestamp:
    query_lines = [
        'last()',
        'map(fn: (r) => ({_value: int(v: r["_time"])}))',
        'max()',
    ]
    tables = self.execute_query(TimestampRange.ETERNITY, query_lines)
    _SINGLE_INT_VALUE_VALIDATOR.validate(tables)

    if len(tables) == 0:
      raise ValueError('no result was returned, is the bucket empty?')
    return Timestamp(int(tables[0]['_value']))

  def copy_to_bucket(self, target_bucket_info: BucketInfo, ts_range: TimestampRange) -> int:
    query_lines = list(self._FLUX_QUERY)
    if not self._DRY_RUN:
      query_lines.append(target_bucket_info.to())
    query_lines.extend([
        'count()',
        'group()',
        'sum()',
    ])

    tables = self.execute_query(ts_range, query_lines)
    _SINGLE_INT_VALUE_VALIDATOR.validate(tables)

    if len(tables) == 0:
      return 0
    return int(tables[0]['_value'])


_LIST_OF_DICTS_VALIDATOR = Draft202012Validator({
    'type': 'array',
    'items': {
        'type': 'object'
    },
})
Draft202012Validator.check_schema(_LIST_OF_DICTS_VALIDATOR.schema)

_SINGLE_INT_VALUE_VALIDATOR = Draft202012Validator({
    'type': 'array',
    'minItems': 0,
    'maxItems': 1,
    'items': {
        'type': 'object',
        'properties': {
            '_value': {
                'type': 'integer'
            }
        },
        'required': ['_value']
    },
})
Draft202012Validator.check_schema(_SINGLE_INT_VALUE_VALIDATOR.schema)
