from threading import Event
from types import SimpleNamespace
from unittest.mock import Mock, patch

from absl.testing import absltest, flagsaver

from line_protocol_cache.bucketmigrationhelper.bucketmigrationhelper import (
    _DST_BUCKET, _DST_ORG, _DST_SERVER_URL, _DST_TOKEN, _FLUX_QUERY, _TIME_RANGE_INCREMENTS_NS, _TIME_RANGE_START_NS,
    _TIME_RANGE_STOP_NS, _WRITE_TO_DST_BUCKET, BucketMigrationHelper)
from line_protocol_cache.bucketmigrationhelper.sourcebucketclient import SourceBucketClient
from line_protocol_cache.bucketmigrationhelper.timestamp import Timestamp


class TestBucketMigrationHelper(absltest.TestCase):
  FAKE_CLIENT = SimpleNamespace(
      execute=Mock(),
      assert_non_empty_bucket=Mock(),
      min_timestamp=Mock(return_value=Timestamp.min()),
      max_timestamp=Mock(return_value=Timestamp.max()),
      only_int_value=Mock(return_value=0),
  )

  def setUp(self):
    self.fake_client_enter = patch.object(
        SourceBucketClient,
        SourceBucketClient.__enter__.__name__,
        Mock(return_value=self.FAKE_CLIENT),
    )
    self.fake_client_enter.__enter__()

    self.fake_client_exit = patch.object(
        SourceBucketClient,
        SourceBucketClient.__exit__.__name__,
        # Return None to not supress the exception.
        # See https://docs.python.org/3/reference/datamodel.html#object.__exit__.
        Mock(return_value=None),
    )
    self.fake_client_exit.__enter__()

    return super().setUp()

  def tearDown(self) -> None:
    self.fake_client_exit.__exit__(None, None, None)
    self.fake_client_enter.__exit__(None, None, None)
    self.FAKE_CLIENT.execute.reset_mock()
    self.FAKE_CLIENT.assert_non_empty_bucket.reset_mock()
    self.FAKE_CLIENT.min_timestamp.reset_mock()
    self.FAKE_CLIENT.max_timestamp.reset_mock()
    self.FAKE_CLIENT.only_int_value.reset_mock()
    return super().tearDown()

  @patch.object(FAKE_CLIENT, 'assert_non_empty_bucket', Mock(side_effect=[ValueError()]))
  def test_emptyBucket_doesNotContinue(self):
    stop_running = Event()
    stop_running.set()

    with self.assertRaises(ValueError):
      BucketMigrationHelper().run(stop_running)

    self.FAKE_CLIENT.assert_non_empty_bucket.assert_called_once()
    self.FAKE_CLIENT.min_timestamp.assert_not_called()
    self.FAKE_CLIENT.max_timestamp.assert_not_called()

  @flagsaver.as_parsed(
      (_TIME_RANGE_START_NS, str(0)),
      (_TIME_RANGE_INCREMENTS_NS, str(Timestamp.max().nanoseconds)),
  )
  def test_timeRangeStartNsSet_usesFlagValue(self):
    BucketMigrationHelper().run()

    self.FAKE_CLIENT.min_timestamp.assert_not_called()
    self.FAKE_CLIENT.execute.assert_called_once()
    self.assertEqual(self.FAKE_CLIENT.execute.call_args.args[0], Timestamp(0))
    self.assertEqual(self.FAKE_CLIENT.execute.call_args.args[1], Timestamp.max())

  @flagsaver.as_parsed(
      (_TIME_RANGE_STOP_NS, str(0)),
      (_TIME_RANGE_INCREMENTS_NS, str(Timestamp.max().nanoseconds)),
  )
  def test_timeRangeStopNsSet_usesFlagValue(self):
    BucketMigrationHelper().run()

    self.FAKE_CLIENT.max_timestamp.assert_not_called()
    self.FAKE_CLIENT.execute.assert_called_once()
    self.assertEqual(self.FAKE_CLIENT.execute.call_args.args[0], Timestamp.min())
    self.assertEqual(self.FAKE_CLIENT.execute.call_args.args[1], Timestamp(0))

  @flagsaver.as_parsed(
      (_TIME_RANGE_START_NS, str(-10)),
      (_TIME_RANGE_STOP_NS, str(10)),
      (_TIME_RANGE_INCREMENTS_NS, str(30)),
  )
  def test_incrementGreaterThanTimeRange_executes1Time(self):
    BucketMigrationHelper().run()

    self.FAKE_CLIENT.execute.assert_called_once()
    self.assertEqual(self.FAKE_CLIENT.execute.call_args.args[0], Timestamp(-10))
    self.assertEqual(self.FAKE_CLIENT.execute.call_args.args[1], Timestamp(10))

  @flagsaver.as_parsed(
      (_TIME_RANGE_START_NS, str(-10)),
      (_TIME_RANGE_STOP_NS, str(10)),
      (_TIME_RANGE_INCREMENTS_NS, str(20)),
  )
  def test_incrementEqualsTimeRange_executes1Time(self):
    BucketMigrationHelper().run()

    self.FAKE_CLIENT.execute.assert_called_once()
    self.assertEqual(self.FAKE_CLIENT.execute.call_args.args[0], Timestamp(-10))
    self.assertEqual(self.FAKE_CLIENT.execute.call_args.args[1], Timestamp(10))

  @flagsaver.as_parsed(
      (_TIME_RANGE_START_NS, str(-10)),
      (_TIME_RANGE_STOP_NS, str(10)),
      (_TIME_RANGE_INCREMENTS_NS, str(15)),
  )
  def test_incrementGreaterThanHalfTimeRange_executes2Times(self):
    BucketMigrationHelper().run()

    self.assertEqual(self.FAKE_CLIENT.execute.call_count, 2)
    self.assertEqual(self.FAKE_CLIENT.execute.call_args_list[0].args[0], Timestamp(-10))
    self.assertEqual(self.FAKE_CLIENT.execute.call_args_list[0].args[1], Timestamp(5))
    self.assertEqual(self.FAKE_CLIENT.execute.call_args_list[1].args[0], Timestamp(5))
    self.assertEqual(self.FAKE_CLIENT.execute.call_args_list[1].args[1], Timestamp(10))

  @flagsaver.as_parsed(
      (_TIME_RANGE_START_NS, str(-10)),
      (_TIME_RANGE_STOP_NS, str(10)),
      (_TIME_RANGE_INCREMENTS_NS, str(10)),
  )
  def test_incrementEqualsHalfTimeRange_executes2Times(self):
    BucketMigrationHelper().run()

    self.assertEqual(self.FAKE_CLIENT.execute.call_count, 2)
    self.assertEqual(self.FAKE_CLIENT.execute.call_args_list[0].args[0], Timestamp(-10))
    self.assertEqual(self.FAKE_CLIENT.execute.call_args_list[0].args[1], Timestamp(0))
    self.assertEqual(self.FAKE_CLIENT.execute.call_args_list[1].args[0], Timestamp(0))
    self.assertEqual(self.FAKE_CLIENT.execute.call_args_list[1].args[1], Timestamp(10))

  @flagsaver.as_parsed(
      (_TIME_RANGE_START_NS, str(-10)),
      (_TIME_RANGE_STOP_NS, str(10)),
      (_TIME_RANGE_INCREMENTS_NS, str(8)),
  )
  def test_incrementLessThanHalfTimeRange_executes3Times(self):
    BucketMigrationHelper().run()

    self.assertEqual(self.FAKE_CLIENT.execute.call_count, 3)
    self.assertEqual(self.FAKE_CLIENT.execute.call_args_list[0].args[0], Timestamp(-10))
    self.assertEqual(self.FAKE_CLIENT.execute.call_args_list[0].args[1], Timestamp(-2))
    self.assertEqual(self.FAKE_CLIENT.execute.call_args_list[1].args[0], Timestamp(-2))
    self.assertEqual(self.FAKE_CLIENT.execute.call_args_list[1].args[1], Timestamp(6))
    self.assertEqual(self.FAKE_CLIENT.execute.call_args_list[2].args[0], Timestamp(6))
    self.assertEqual(self.FAKE_CLIENT.execute.call_args_list[2].args[1], Timestamp(10))

  @flagsaver.as_parsed(
      (_TIME_RANGE_START_NS, str(-10)),
      (_TIME_RANGE_STOP_NS, str(10)),
      (_TIME_RANGE_INCREMENTS_NS, str(20)),
      (_FLUX_QUERY, ['function1()', 'function2()']),
  )
  def test_fluxQuerySet_includedInQueryLines(self):
    BucketMigrationHelper().run()

    self.FAKE_CLIENT.execute.assert_called_once()
    self.assertIn('function1()', self.FAKE_CLIENT.execute.call_args.args[2])
    self.assertIn('function2()', self.FAKE_CLIENT.execute.call_args.args[2])

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

    self.FAKE_CLIENT.execute.assert_called_once()
    self.assertNotIn('to(host: "url", bucket: "bucket", org: "org", token: "token")',
                     self.FAKE_CLIENT.execute.call_args.args[2])
    self.assertIn(None, self.FAKE_CLIENT.execute.call_args.args[2])

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

    self.FAKE_CLIENT.execute.assert_called_once()
    self.assertIn('to(host: "url", bucket: "bucket", org: "org", token: "token")',
                  self.FAKE_CLIENT.execute.call_args.args[2])

  @flagsaver.as_parsed(
      (_TIME_RANGE_START_NS, str(-10)),
      (_TIME_RANGE_STOP_NS, str(10)),
      (_TIME_RANGE_INCREMENTS_NS, str(8)),
  )
  def test_stopRunningSet_returnsEarly(self):
    stop_running = Event()
    stop_running.is_set = Mock(side_effect=[False, False, True])

    BucketMigrationHelper().run(stop_running)

    self.assertEqual(self.FAKE_CLIENT.execute.call_count, 2)
