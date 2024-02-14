from threading import Event
from unittest.mock import Mock, patch

from absl.testing import absltest, flagsaver

from line_protocol_cache.bucketmigrationhelper.bucketmigrationhelper import (
    _DST_BUCKET, _DST_ORG, _DST_SERVER_URL, _DST_TOKEN, _FLUX_QUERY, _TIME_RANGE_INCREMENTS_NS, _TIME_RANGE_START_NS,
    _TIME_RANGE_STOP_NS, _WRITE_TO_DST_BUCKET, BucketMigrationHelper)
from line_protocol_cache.bucketmigrationhelper.sourcebucketclient import SourceBucketClient
from line_protocol_cache.bucketmigrationhelper.timestamp import Timestamp

MOCK_CLIENT = Mock(
    spec=SourceBucketClient,
    min_timestamp=Mock(return_value=Timestamp.min()),
    max_timestamp=Mock(return_value=Timestamp.max()),
    only_int_value=Mock(return_value=0),
)


@patch.object(SourceBucketClient, SourceBucketClient.__enter__.__name__, Mock(return_value=MOCK_CLIENT))
@patch.object(SourceBucketClient, SourceBucketClient.__exit__.__name__, Mock(return_value=None))
class TestBucketMigrationHelper(absltest.TestCase):

  def tearDown(self) -> None:
    MOCK_CLIENT.reset_mock()
    return super().tearDown()

  @patch.object(MOCK_CLIENT, SourceBucketClient.assert_non_empty_bucket.__name__, Mock(side_effect=[ValueError()]))
  def test_emptyBucket_doesNotContinue(self):
    stop_running = Event()
    stop_running.set()

    with self.assertRaises(ValueError):
      BucketMigrationHelper().run(stop_running)

    MOCK_CLIENT.assert_non_empty_bucket.assert_called_once()
    MOCK_CLIENT.min_timestamp.assert_not_called()
    MOCK_CLIENT.max_timestamp.assert_not_called()

  @flagsaver.as_parsed(
      (_TIME_RANGE_START_NS, str(0)),
      (_TIME_RANGE_INCREMENTS_NS, str(Timestamp.max().nanoseconds)),
  )
  def test_timeRangeStartNsSet_usesFlagValue(self):
    BucketMigrationHelper().run()

    MOCK_CLIENT.min_timestamp.assert_not_called()
    MOCK_CLIENT.execute.assert_called_once()
    self.assertEqual(MOCK_CLIENT.execute.call_args.args[0], Timestamp(0))
    self.assertEqual(MOCK_CLIENT.execute.call_args.args[1], Timestamp.max())

  @flagsaver.as_parsed(
      (_TIME_RANGE_STOP_NS, str(0)),
      (_TIME_RANGE_INCREMENTS_NS, str(Timestamp.max().nanoseconds)),
  )
  def test_timeRangeStopNsSet_usesFlagValue(self):
    BucketMigrationHelper().run()

    MOCK_CLIENT.max_timestamp.assert_not_called()
    MOCK_CLIENT.execute.assert_called_once()
    self.assertEqual(MOCK_CLIENT.execute.call_args.args[0], Timestamp.min())
    self.assertEqual(MOCK_CLIENT.execute.call_args.args[1], Timestamp(0))

  @flagsaver.as_parsed(
      (_TIME_RANGE_START_NS, str(-10)),
      (_TIME_RANGE_STOP_NS, str(10)),
      (_TIME_RANGE_INCREMENTS_NS, str(30)),
  )
  def test_incrementGreaterThanTimeRange_executes1Time(self):
    BucketMigrationHelper().run()

    MOCK_CLIENT.execute.assert_called_once()
    self.assertEqual(MOCK_CLIENT.execute.call_args.args[0], Timestamp(-10))
    self.assertEqual(MOCK_CLIENT.execute.call_args.args[1], Timestamp(10))

  @flagsaver.as_parsed(
      (_TIME_RANGE_START_NS, str(-10)),
      (_TIME_RANGE_STOP_NS, str(10)),
      (_TIME_RANGE_INCREMENTS_NS, str(20)),
  )
  def test_incrementEqualsTimeRange_executes1Time(self):
    BucketMigrationHelper().run()

    MOCK_CLIENT.execute.assert_called_once()
    self.assertEqual(MOCK_CLIENT.execute.call_args.args[0], Timestamp(-10))
    self.assertEqual(MOCK_CLIENT.execute.call_args.args[1], Timestamp(10))

  @flagsaver.as_parsed(
      (_TIME_RANGE_START_NS, str(-10)),
      (_TIME_RANGE_STOP_NS, str(10)),
      (_TIME_RANGE_INCREMENTS_NS, str(15)),
  )
  def test_incrementGreaterThanHalfTimeRange_executes2Times(self):
    BucketMigrationHelper().run()

    self.assertEqual(MOCK_CLIENT.execute.call_count, 2)
    self.assertEqual(MOCK_CLIENT.execute.call_args_list[0].args[0], Timestamp(-10))
    self.assertEqual(MOCK_CLIENT.execute.call_args_list[0].args[1], Timestamp(5))
    self.assertEqual(MOCK_CLIENT.execute.call_args_list[1].args[0], Timestamp(5))
    self.assertEqual(MOCK_CLIENT.execute.call_args_list[1].args[1], Timestamp(10))

  @flagsaver.as_parsed(
      (_TIME_RANGE_START_NS, str(-10)),
      (_TIME_RANGE_STOP_NS, str(10)),
      (_TIME_RANGE_INCREMENTS_NS, str(10)),
  )
  def test_incrementEqualsHalfTimeRange_executes2Times(self):
    BucketMigrationHelper().run()

    self.assertEqual(MOCK_CLIENT.execute.call_count, 2)
    self.assertEqual(MOCK_CLIENT.execute.call_args_list[0].args[0], Timestamp(-10))
    self.assertEqual(MOCK_CLIENT.execute.call_args_list[0].args[1], Timestamp(0))
    self.assertEqual(MOCK_CLIENT.execute.call_args_list[1].args[0], Timestamp(0))
    self.assertEqual(MOCK_CLIENT.execute.call_args_list[1].args[1], Timestamp(10))

  @flagsaver.as_parsed(
      (_TIME_RANGE_START_NS, str(-10)),
      (_TIME_RANGE_STOP_NS, str(10)),
      (_TIME_RANGE_INCREMENTS_NS, str(8)),
  )
  def test_incrementLessThanHalfTimeRange_executes3Times(self):
    BucketMigrationHelper().run()

    self.assertEqual(MOCK_CLIENT.execute.call_count, 3)
    self.assertEqual(MOCK_CLIENT.execute.call_args_list[0].args[0], Timestamp(-10))
    self.assertEqual(MOCK_CLIENT.execute.call_args_list[0].args[1], Timestamp(-2))
    self.assertEqual(MOCK_CLIENT.execute.call_args_list[1].args[0], Timestamp(-2))
    self.assertEqual(MOCK_CLIENT.execute.call_args_list[1].args[1], Timestamp(6))
    self.assertEqual(MOCK_CLIENT.execute.call_args_list[2].args[0], Timestamp(6))
    self.assertEqual(MOCK_CLIENT.execute.call_args_list[2].args[1], Timestamp(10))

  @flagsaver.as_parsed(
      (_TIME_RANGE_START_NS, str(-10)),
      (_TIME_RANGE_STOP_NS, str(10)),
      (_TIME_RANGE_INCREMENTS_NS, str(20)),
      (_FLUX_QUERY, ['function1()', 'function2()']),
  )
  def test_fluxQuerySet_includedInQueryLines(self):
    BucketMigrationHelper().run()

    MOCK_CLIENT.execute.assert_called_once()
    self.assertIn('function1()', MOCK_CLIENT.execute.call_args.args[2])
    self.assertIn('function2()', MOCK_CLIENT.execute.call_args.args[2])

  @flagsaver.as_parsed(
      (_TIME_RANGE_START_NS, str(-10)),
      (_TIME_RANGE_STOP_NS, str(10)),
      (_TIME_RANGE_INCREMENTS_NS, str(20)),
      (_DST_SERVER_URL, 'url'),
      (_DST_BUCKET, 'bucket'),
      (_DST_ORG, 'org'),
      (_DST_TOKEN, 'token'),
  )
  def test_writeToDstBucketNotSet_doesNotWrite(self):
    BucketMigrationHelper().run()

    MOCK_CLIENT.execute.assert_called_once()
    self.assertNotIn('to(host: "url", bucket: "bucket", org: "org", token: "token")',
                     MOCK_CLIENT.execute.call_args.args[2])
    self.assertIn(None, MOCK_CLIENT.execute.call_args.args[2])

  @flagsaver.as_parsed(
      (_TIME_RANGE_START_NS, str(-10)),
      (_TIME_RANGE_STOP_NS, str(10)),
      (_TIME_RANGE_INCREMENTS_NS, str(20)),
      (_DST_SERVER_URL, 'url'),
      (_DST_BUCKET, 'bucket'),
      (_DST_ORG, 'org'),
      (_DST_TOKEN, 'token'),
      (_WRITE_TO_DST_BUCKET, str(True)),
  )
  def test_writeToDstBucketSet_writes(self):
    BucketMigrationHelper().run()

    MOCK_CLIENT.execute.assert_called_once()
    self.assertIn('to(host: "url", bucket: "bucket", org: "org", token: "token")',
                  MOCK_CLIENT.execute.call_args.args[2])

  @flagsaver.as_parsed(
      (_TIME_RANGE_START_NS, str(-10)),
      (_TIME_RANGE_STOP_NS, str(10)),
      (_TIME_RANGE_INCREMENTS_NS, str(8)),
  )
  def test_stopRunningSet_returnsEarly(self):
    stop_running = Event()
    stop_running.is_set = Mock(side_effect=[False, False, True])

    BucketMigrationHelper().run(stop_running)

    self.assertEqual(MOCK_CLIENT.execute.call_count, 2)
