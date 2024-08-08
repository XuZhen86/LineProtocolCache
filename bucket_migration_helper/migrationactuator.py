from threading import Event

from absl import flags, logging

from common.bucketinfo import BucketInfo

from .bucketclient import BucketClient
from .duration import Duration
from .timestamp import Timestamp
from .timestamprange import TimestampRange
from .timestamprangeiterator import TimestampRangeIterator

_SOURCE_SERVER_URL = flags.DEFINE_string(
    name='source_server_url',
    default=None,
    required=True,
    help='InfluxDB server API URL of the source bucket (ex. http://localhost:8086).',
)
_SOURCE_ORG = flags.DEFINE_string(
    name='source_org',
    default=None,
    required=True,
    help='Organization name of the source bucket.',
)
_SOURCE_BUCKET = flags.DEFINE_string(
    name='source_bucket',
    default=None,
    required=True,
    help='Name of the source bucket.',
)
_SOURCE_TOKEN = flags.DEFINE_string(
    name='source_token',
    default=None,
    required=True,
    help='Token to access the source bucket.',
)

_TARGET_SERVER_URL = flags.DEFINE_string(
    name='target_server_url',
    default=None,
    required=True,
    help='InfluxDB server API URL of the target bucket (ex. http://localhost:8086).',
)
_TARGET_ORG = flags.DEFINE_string(
    name='target_org',
    default=None,
    required=True,
    help='Organization name of the target bucket.',
)
_TARGET_BUCKET = flags.DEFINE_string(
    name='target_bucket',
    default=None,
    required=True,
    help='Name of the target bucket.',
)
_TARGET_TOKEN = flags.DEFINE_string(
    name='target_token',
    default=None,
    required=True,
    help='Token to access the target bucket.',
)

_TIME_RANGE_START = flags.DEFINE_string(
    name='time_range_start',
    default=None,
    help='Time range starting point for the source data points. '
    'INCLUDES the records with _time that matches the timestamp. '
    'If not provided, the script starts from the first record.',
)
_TIME_RANGE_STOP = flags.DEFINE_string(
    name='time_range_stop',
    default=None,
    help='Time range stopping point for the source data points. '
    'EXCLUDES the records with _time that matches the timestamp. '
    'If not provided, the script stops after the last record.',
)
_TIME_RANGE_INCREMENT = flags.DEFINE_string(
    name='time_range_increment',
    default=str(Duration.HOUR),
    help='Time range increments between each query. '
    'Reduce the increment if the query times out.',
)


class MigrationActuator:

  def __init__(self) -> None:
    self._SOURCE_SERVER_URL = _SOURCE_SERVER_URL.value if _SOURCE_SERVER_URL.present else _SOURCE_SERVER_URL.default
    self._SOURCE_ORG = _SOURCE_ORG.value if _SOURCE_ORG.present else _SOURCE_ORG.default
    self._SOURCE_BUCKET = _SOURCE_BUCKET.value if _SOURCE_BUCKET.present else _SOURCE_BUCKET.default
    self._SOURCE_TOKEN = _SOURCE_TOKEN.value if _SOURCE_TOKEN.present else _SOURCE_TOKEN.default
    self._TARGET_SERVER_URL = _TARGET_SERVER_URL.value if _TARGET_SERVER_URL.present else _TARGET_SERVER_URL.default
    self._TARGET_ORG = _TARGET_ORG.value if _TARGET_ORG.present else _TARGET_ORG.default
    self._TARGET_BUCKET = _TARGET_BUCKET.value if _TARGET_BUCKET.present else _TARGET_BUCKET.default
    self._TARGET_TOKEN = _TARGET_TOKEN.value if _TARGET_TOKEN.present else _TARGET_TOKEN.default
    self._TIME_RANGE_START = _TIME_RANGE_START.value if _TIME_RANGE_START.present else _TIME_RANGE_START.default
    self._TIME_RANGE_STOP = _TIME_RANGE_STOP.value if _TIME_RANGE_STOP.present else _TIME_RANGE_STOP.default
    self._TIME_RANGE_INCREMENT = _TIME_RANGE_INCREMENT.value if _TIME_RANGE_INCREMENT.present else _TIME_RANGE_INCREMENT.default

  def run(self, stop_event: Event) -> None:
    soruce_bucket_info = BucketInfo(server_url=self._SOURCE_SERVER_URL,
                                    organization=self._SOURCE_ORG,
                                    bucket=self._SOURCE_BUCKET,
                                    token=self._SOURCE_TOKEN)
    target_bucket_info = BucketInfo(server_url=self._TARGET_SERVER_URL,
                                    organization=self._TARGET_ORG,
                                    bucket=self._TARGET_BUCKET,
                                    token=self._TARGET_TOKEN)
    duration = Duration.build(self._TIME_RANGE_INCREMENT)

    with BucketClient(soruce_bucket_info) as client:
      if self._TIME_RANGE_START is not None:
        start = Timestamp.build(self._TIME_RANGE_START)
      else:
        start = client.get_min_timestamp()

      if self._TIME_RANGE_STOP is not None:
        stop = Timestamp.build(self._TIME_RANGE_STOP)
      else:
        stop = client.get_max_timestamp()
        # +1ns to include the record with the last timestamp, since 'stop' is exclusive.
        try:
          stop += Duration(1)
        except ValueError:
          pass

      ts_range = TimestampRange(start, stop)
      duration = Duration.build(self._TIME_RANGE_INCREMENT)
      iterator = TimestampRangeIterator(ts_range, duration)

      logging.info(f'Iterating from {ts_range.start} ({ts_range.start.instant_ns}), '
                   f'to {ts_range.stop} ({ts_range.stop.instant_ns}), '
                   f'interval {duration}')

      for i, current_range in enumerate(iterator):
        if stop_event.is_set():
          break

        logging.info(f'{i} of {iterator.length()} iterations, '
                     f'from {current_range.start} ({current_range.start.instant_ns}), '
                     f'to {current_range.stop} ({current_range.stop.instant_ns})')

        record_count = client.copy_to_bucket(target_bucket_info, current_range)

        logging.info(f'Copied {record_count} records')
