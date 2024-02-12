import math
import signal
from threading import Event

from absl import app, flags, logging

from line_protocol_cache.bucketmigrationhelper.sourcebucketclient import SourceBucketClient
from line_protocol_cache.bucketmigrationhelper.timestamp import Timestamp

_WRITE_TO_DST_BUCKET = flags.DEFINE_bool(
    name='write_to_dst_bucket',
    default=False,
    help='Controls if the queried records are written to the destination bucket. '
    'This flag does not gurantee anything in flag "flux_query" would not modify data. '
    'Set this flag to True for the migration.',
)

_DST_SERVER_URL = flags.DEFINE_string(
    name='dst_server_url',
    default=None,
    required=False,
    help='InfluxDB server API URL for the destination bucket (ex. http://localhost:8086).',
)
_DST_BUCKET = flags.DEFINE_string(
    name='dst_bucket',
    default=None,
    required=False,
    help='Specifies the destination bucket for reading the metrics from.',
)
_DST_TOKEN = flags.DEFINE_string(
    name='dst_token',
    default=None,
    required=False,
    help='Token to authenticate to read the destination bucket.',
)
_DST_ORG = flags.DEFINE_string(
    name='dst_org',
    default=None,
    required=False,
    help='Organization name of the destination bucket.',
)

_FLUX_QUERY = flags.DEFINE_multi_string(
    name='flux_query',
    default=[],
    help='Lines of Flux query to process the source data points before sending them to the destination bucket. '
    'Use --dry_run=true and --verbosity=1 to preview the assembled queries.',
)

_TIME_RANGE_START_NS = flags.DEFINE_integer(
    name='time_range_start_ns',
    default=Timestamp.min().nanoseconds,
    lower_bound=Timestamp.min().nanoseconds,
    upper_bound=Timestamp.max().nanoseconds,
    help='Time range starting point for the source data points, specified in nanoseconds. '
    'Includes the records with _time that matches the timestamp. '
    'If not provided, the script starts from the first record.',
)
_TIME_RANGE_STOP_NS = flags.DEFINE_integer(
    name='time_range_stop_ns',
    default=Timestamp.max().nanoseconds,
    lower_bound=Timestamp.min().nanoseconds,
    upper_bound=Timestamp.max().nanoseconds,
    help='Time range stopping point for the source data points, specified in nanoseconds. '
    'Excludes the records with _time that matches the timestamp. '
    'If not provided, the script stops after the last record.',
)
_TIME_RANGE_INCREMENTS_NS = flags.DEFINE_integer(
    name='time_range_increment_ns',
    default=60 * 60 * 10**9,  # 1 hour.
    lower_bound=1,
    upper_bound=Timestamp.max().nanoseconds,
    help='Time range increments between each query. '
    'Reduce the increment if the query times out. '
    'Default 1 hour.',
)


class BucketMigrationHelper:

  def _copy_points(self, client: SourceBucketClient, range_start: Timestamp, range_stop: Timestamp) -> int:
    if (_WRITE_TO_DST_BUCKET.value if _WRITE_TO_DST_BUCKET.present else _WRITE_TO_DST_BUCKET.default):
      assert _DST_SERVER_URL.value is not None
      assert _DST_BUCKET.value is not None
      assert _DST_ORG.value is not None
      assert _DST_TOKEN.value is not None
      to_bucket = f'to(host: "{_DST_SERVER_URL.value}", bucket: "{_DST_BUCKET.value}", org: "{_DST_ORG.value}", token: "{_DST_TOKEN.value}")'
    else:
      to_bucket = None

    query_lines: list[str | None] = [
        'drop(columns: ["_start", "_stop"])',
        *(_FLUX_QUERY.value if _FLUX_QUERY.present else _FLUX_QUERY.default),
        to_bucket,
        'count()',
        'group()',
        'sum()',
    ]

    tables = client.execute(range_start, range_stop, query_lines)
    return client.only_int_value(tables, 0)

  def run(self, stop_running: Event = Event()) -> None:
    with SourceBucketClient() as client:
      client.assert_non_empty_bucket()

      min_timestamp = Timestamp(_TIME_RANGE_START_NS.value) if _TIME_RANGE_START_NS.present else client.min_timestamp()
      max_timestamp = Timestamp(_TIME_RANGE_STOP_NS.value) if _TIME_RANGE_STOP_NS.present else client.max_timestamp()
      logging.info(f'Iterating from {min_timestamp}, {min_timestamp.nanoseconds} '
                   f'to {max_timestamp}, {max_timestamp.nanoseconds}')

      increment_ns = _TIME_RANGE_INCREMENTS_NS.value if _TIME_RANGE_INCREMENTS_NS.present else _TIME_RANGE_INCREMENTS_NS.default
      iterations = math.ceil((max_timestamp.nanoseconds - min_timestamp.nanoseconds) / increment_ns)

      for i in range(iterations):
        if stop_running.is_set():
          return

        range_start = min_timestamp + i * increment_ns
        range_stop = min(range_start + increment_ns, max_timestamp)

        count = self._copy_points(client, range_start, range_stop)
        logging.info(f'{i} of {iterations}. {count} records. '
                     f'From {range_start}, {range_start.nanoseconds} '
                     f'to {range_stop}, {range_stop.nanoseconds}.')


def main(args: list[str]) -> None:
  stop_running = Event()
  signal.signal(signal.SIGTERM, lambda signal_number, stack_frame: stop_running.set())

  BucketMigrationHelper().run(stop_running)


def app_run_main() -> None:
  app.run(main)
