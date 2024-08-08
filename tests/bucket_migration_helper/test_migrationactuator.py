from threading import Event
from unittest.mock import Mock, call, patch

from absl import logging
from absl.logging.converter import absl_to_standard
from absl.testing import flagsaver, parameterized

from bucket_migration_helper.bucketclient import BucketClient
from bucket_migration_helper.duration import Duration
from bucket_migration_helper.migrationactuator import (_SOURCE_BUCKET, _SOURCE_ORG, _SOURCE_SERVER_URL, _SOURCE_TOKEN,
                                                       _TARGET_BUCKET, _TARGET_ORG, _TARGET_SERVER_URL, _TARGET_TOKEN,
                                                       _TIME_RANGE_INCREMENT, _TIME_RANGE_START, _TIME_RANGE_STOP,
                                                       MigrationActuator)
from bucket_migration_helper.timestamp import Timestamp
from bucket_migration_helper.timestamprange import TimestampRange
from common.bucketinfo import BucketInfo

MOCK_GET_MIN_TIMESTAMP = Mock()
MOCK_GET_MAX_TIMESTAMP = Mock()
MOCK_COPY_TO_BUCKET = Mock()
MOCK_BUCKET_CLIENT = Mock(
    spec=BucketClient,
    get_min_timestamp=MOCK_GET_MIN_TIMESTAMP,
    get_max_timestamp=MOCK_GET_MAX_TIMESTAMP,
    copy_to_bucket=MOCK_COPY_TO_BUCKET,
)

MOCK_IS_SET = Mock()
MOCK_STOP_EVENT = Mock(spec=Event, is_set=MOCK_IS_SET)

SOURCE_BUCKET_INFO = BucketInfo(server_url='source-server-url',
                                organization='source-org',
                                bucket='source-bucket',
                                token='source-token')
TARGET_BUCKET_INFO = BucketInfo(server_url='target-server-url',
                                organization='target-org',
                                bucket='target-bucket',
                                token='target-token')


@patch.object(BucketClient, BucketClient.__enter__.__name__, Mock(return_value=MOCK_BUCKET_CLIENT))
@patch.object(BucketClient, BucketClient.__exit__.__name__, Mock(return_value=None))
class TestMirgationActuator(parameterized.TestCase):

  def setUp(self):
    self.saved_flags = flagsaver.as_parsed(
        (_SOURCE_BUCKET, SOURCE_BUCKET_INFO.bucket),
        (_SOURCE_ORG, SOURCE_BUCKET_INFO.organization),
        (_SOURCE_SERVER_URL, SOURCE_BUCKET_INFO.server_url),
        (_SOURCE_TOKEN, SOURCE_BUCKET_INFO.token),
        (_TARGET_BUCKET, TARGET_BUCKET_INFO.bucket),
        (_TARGET_ORG, TARGET_BUCKET_INFO.organization),
        (_TARGET_SERVER_URL, TARGET_BUCKET_INFO.server_url),
        (_TARGET_TOKEN, TARGET_BUCKET_INFO.token),
    )
    self.saved_flags.__enter__()

    MOCK_GET_MIN_TIMESTAMP.return_value = Timestamp(-10)
    MOCK_GET_MAX_TIMESTAMP.return_value = Timestamp(10)
    MOCK_COPY_TO_BUCKET.return_value = 0

    MOCK_IS_SET.side_effect = [False, True]

    return super().setUp()

  def tearDown(self) -> None:
    self.saved_flags.__exit__(None, None, None)

    MOCK_BUCKET_CLIENT.reset_mock(return_value=True, side_effect=True)
    MOCK_STOP_EVENT.reset_mock(return_value=True, side_effect=True)

    return super().tearDown()

  def test_noTimeRangeStart_queriesSourceBucket(self):
    MOCK_GET_MIN_TIMESTAMP.return_value = Timestamp(-69420)

    MigrationActuator().run(MOCK_STOP_EVENT)

    MOCK_GET_MIN_TIMESTAMP.assert_called_once_with()
    MOCK_COPY_TO_BUCKET.assert_called_once_with(TARGET_BUCKET_INFO, TimestampRange(Timestamp(-69420), Timestamp(11)))

  @flagsaver.as_parsed((_TIME_RANGE_START, '-1234'))
  def test_hasTimeRangeStart_usesFlagValue(self):
    MigrationActuator().run(MOCK_STOP_EVENT)

    MOCK_GET_MIN_TIMESTAMP.assert_not_called()
    MOCK_COPY_TO_BUCKET.assert_called_once_with(TARGET_BUCKET_INFO, TimestampRange(Timestamp(-1234), Timestamp(11)))

  def test_noTimeRangeStop_queriesSourceBucketAndPlus1(self):
    MOCK_GET_MAX_TIMESTAMP.return_value = Timestamp(69420)

    MigrationActuator().run(MOCK_STOP_EVENT)

    MOCK_GET_MAX_TIMESTAMP.assert_called_once_with()
    MOCK_COPY_TO_BUCKET.assert_called_once_with(TARGET_BUCKET_INFO, TimestampRange(Timestamp(-10), Timestamp(69421)))

  @flagsaver.as_parsed((_TIME_RANGE_INCREMENT, str(Duration.MAX.duration_ns)))
  def test_noTimeRangeStop_toleratesMaxTimestamp(self):
    MOCK_GET_MAX_TIMESTAMP.return_value = Timestamp.MAX

    MigrationActuator().run(MOCK_STOP_EVENT)

    MOCK_GET_MAX_TIMESTAMP.assert_called_once_with()
    MOCK_COPY_TO_BUCKET.assert_called_once_with(TARGET_BUCKET_INFO, TimestampRange(Timestamp(-10), Timestamp.MAX))

  @flagsaver.as_parsed((_TIME_RANGE_STOP, '1234'))
  def test_hasTimeRangeStop_usesFlagValue(self):
    MigrationActuator().run(MOCK_STOP_EVENT)

    MOCK_GET_MAX_TIMESTAMP.assert_not_called()
    MOCK_COPY_TO_BUCKET.assert_called_once_with(TARGET_BUCKET_INFO, TimestampRange(Timestamp(-10), Timestamp(1234)))

  @flagsaver.as_parsed(
      (_TIME_RANGE_START, '1234'),
      (_TIME_RANGE_STOP, '-1234'),
  )
  def test_invalidTsRange_raises(self):
    with self.assertRaises(Exception):
      MigrationActuator().run(MOCK_STOP_EVENT)

  @flagsaver.as_parsed((_TIME_RANGE_INCREMENT, '-1'))
  def test_invalidDuration_raises(self):
    with self.assertRaises(Exception):
      MigrationActuator().run(MOCK_STOP_EVENT)

  def test_logsBeforeIterations(self):
    with self.assertLogs(logger='absl', level=absl_to_standard(logging.INFO)) as logs:
      MigrationActuator().run(MOCK_STOP_EVENT)

    self.assertIn(
        'Iterating from 1969-12-31T23:59:59.999999990Z (-10), to 1970-01-01T00:00:00.000000011Z (11), interval +01:00:00.000000000',
        [record.message for record in logs.records])

  @flagsaver.as_parsed(
      (_TIME_RANGE_START, '-1000'),
      (_TIME_RANGE_STOP, '100'),
      (_TIME_RANGE_INCREMENT, '10'),
  )
  def test_stopsWhenStopEventIsSet(self):
    MOCK_IS_SET.side_effect = [False, False, False, True]

    MigrationActuator().run(MOCK_STOP_EVENT)

    MOCK_COPY_TO_BUCKET.assert_has_calls([
        call(TARGET_BUCKET_INFO, TimestampRange(Timestamp(-1000), Timestamp(-990))),
        call(TARGET_BUCKET_INFO, TimestampRange(Timestamp(-990), Timestamp(-980))),
        call(TARGET_BUCKET_INFO, TimestampRange(Timestamp(-980), Timestamp(-970))),
    ])

  @flagsaver.as_parsed(
      (_TIME_RANGE_START, '-1000'),
      (_TIME_RANGE_STOP, '100'),
      (_TIME_RANGE_INCREMENT, '10'),
  )
  def test_logsRecordCount(self):
    MOCK_IS_SET.side_effect = [False, False, False, True]
    MOCK_COPY_TO_BUCKET.side_effect = [12, 23, 34, 45]

    with self.assertLogs(logger='absl', level=absl_to_standard(logging.INFO)) as logs:
      MigrationActuator().run(MOCK_STOP_EVENT)

    self.assertContainsSubsequence([record.message for record in logs.records],
                                   ['Copied 12 records', 'Copied 23 records', 'Copied 34 records'])

  @flagsaver.as_parsed(
      (_TIME_RANGE_START, '-100'),
      (_TIME_RANGE_STOP, '100'),
      (_TIME_RANGE_INCREMENT, '70'),
  )
  def test_exhaustsIterations(self):
    MOCK_IS_SET.side_effect = [False, False, False, False, True]

    MigrationActuator().run(MOCK_STOP_EVENT)

    MOCK_COPY_TO_BUCKET.assert_has_calls([
        call(TARGET_BUCKET_INFO, TimestampRange(Timestamp(-100), Timestamp(-30))),
        call(TARGET_BUCKET_INFO, TimestampRange(Timestamp(-30), Timestamp(40))),
        call(TARGET_BUCKET_INFO, TimestampRange(Timestamp(40), Timestamp(100))),
    ])
