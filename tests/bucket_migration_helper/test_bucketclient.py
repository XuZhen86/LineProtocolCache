from unittest.mock import Mock, patch

from absl import logging
from absl.logging.converter import absl_to_standard
from absl.testing import flagsaver, parameterized
from influxdb_client import InfluxDBClient, QueryApi
from influxdb_client.client.flux_table import FluxRecord, FluxTable, TableList
from jsonschema import ValidationError
from tenacity import stop_after_attempt, wait_none

from bucket_migration_helper.bucketclient import _DRY_RUN, _FLUX_QUERY, BucketClient
from bucket_migration_helper.timestamp import Timestamp
from bucket_migration_helper.timestamprange import TimestampRange
from common.bucketinfo import BucketInfo

MOCK_QUERY = Mock()


@patch.object(InfluxDBClient, InfluxDBClient.query_api.__name__,
              Mock(return_value=Mock(spec=QueryApi, query=MOCK_QUERY)))
@patch.object(BucketClient.execute_query.retry, 'wait', wait_none())  # type: ignore
@patch.object(BucketClient.execute_query.retry, 'stop', stop_after_attempt(3))  # type: ignore
class TestBucketClient(parameterized.TestCase):

  SOURCE_BUCKET_INFO = BucketInfo(server_url='source-server-url',
                                  organization='source-org',
                                  bucket='source-bucket',
                                  token='source-token')
  TARGET_BUCKET_INFO = BucketInfo(server_url='target-server-url',
                                  organization='target-org',
                                  bucket='target-bucket',
                                  token='target-token')

  def setUp(self):
    MOCK_QUERY.return_value = self.create_table_list_of([])
    return super().setUp()

  def tearDown(self) -> None:
    MOCK_QUERY.reset_mock(return_value=True, side_effect=True)
    return super().tearDown()

  @classmethod
  def create_table_list_of(cls, values: list[int]) -> TableList:
    table_list = TableList()

    for i, value in enumerate(values):
      record = FluxRecord('record')
      record['result'] = '_result'
      record['table'] = i
      record['_value'] = value

      table = FluxTable()
      table.records.append(record)
      table_list.append(table)

    return table_list

  @patch.object(InfluxDBClient, InfluxDBClient.close.__name__, Mock())
  def test_closesClientAtExit(self):
    with BucketClient(self.SOURCE_BUCKET_INFO):
      pass

    InfluxDBClient.close.assert_called_once()  # type: ignore

  def test_executeQuery_callsQueryApiAndLogs(self):
    with self.assertLogs(logger='absl', level=absl_to_standard(logging.DEBUG)) as logs:
      with BucketClient(self.SOURCE_BUCKET_INFO) as client:
        client.execute_query(TimestampRange.ETERNITY, ['query()', 'lines()'])

    MOCK_QUERY.assert_called_once_with(
        'from(bucket: "source-bucket")'
        ' |> range(start: 1677-09-21T00:12:43.145224193Z, stop: 2262-04-11T23:47:16.854775807Z)'
        ' |> query()'
        ' |> lines()'
        ' |> yield()')
    self.assertContainsSubsequence([record.message for record in logs.records], [
        "query='"
        'from(bucket: "source-bucket")'
        ' |> range(start: 1677-09-21T00:12:43.145224193Z, stop: 2262-04-11T23:47:16.854775807Z)'
        ' |> query()'
        ' |> lines()'
        ' |> yield()'
        "'"
    ])

  def test_executeQuery_retriesAndLogs(self):
    MOCK_QUERY.side_effect = [Exception('exception'), ValueError('value-error'), self.create_table_list_of([])]

    with self.assertLogs(logger='absl', level=absl_to_standard(logging.WARNING)) as logs:
      with BucketClient(self.SOURCE_BUCKET_INFO) as client:
        client.execute_query(TimestampRange.ETERNITY, ['query()', 'lines()'])

    self.assertContainsSubsequence([record.message for record in logs.records], [
        'Retrying bucket_migration_helper.bucketclient.BucketClient.execute_query in 0.0 seconds as it raised Exception: exception.',
        'Retrying bucket_migration_helper.bucketclient.BucketClient.execute_query in 0.0 seconds as it raised ValueError: value-error.',
    ])

  def test_executeQuery_reraisesLastException(self):
    MOCK_QUERY.side_effect = [Exception('exception'), ValueError('value-error'), IndexError('index-error')]

    with self.assertLogs(logger='absl', level=absl_to_standard(logging.WARNING)):
      with self.assertRaisesRegex(IndexError, 'index-error'):
        with BucketClient(self.SOURCE_BUCKET_INFO) as client:
          client.execute_query(TimestampRange.ETERNITY, ['query()', 'lines()'])

  def test_isEmpty_emptyBucket_returnsTrue(self):
    with BucketClient(self.SOURCE_BUCKET_INFO) as client:
      self.assertTrue(client.is_emtpy())

  def test_isEmpty_nonEmptyBucket_returnsFalse(self):
    MOCK_QUERY.return_value = self.create_table_list_of([1])

    with BucketClient(self.SOURCE_BUCKET_INFO) as client:
      self.assertFalse(client.is_emtpy())

  def test_getMinTimestamp_emptyBucket_raises(self):
    with self.assertRaisesRegex(ValueError, 'no result was returned, is the bucket empty'):
      with BucketClient(self.SOURCE_BUCKET_INFO) as client:
        client.get_min_timestamp()

  def test_getMinTimestamp_invalidResult_raises(self):
    MOCK_QUERY.return_value = self.create_table_list_of([100, 200])

    with self.assertRaisesRegex(ValidationError, "Failed validating 'maxItems' in schema"):
      with BucketClient(self.SOURCE_BUCKET_INFO) as client:
        client.get_min_timestamp()

  def test_getMinTimestamp_nonEmptyBucket_returnsTimestamp(self):
    MOCK_QUERY.return_value = self.create_table_list_of([100])

    with BucketClient(self.SOURCE_BUCKET_INFO) as client:
      self.assertEqual(client.get_min_timestamp(), Timestamp(100))

  def test_getMaxTimestamp_emptyBucket_raises(self):
    with self.assertRaisesRegex(ValueError, 'no result was returned, is the bucket empty'):
      with BucketClient(self.SOURCE_BUCKET_INFO) as client:
        client.get_max_timestamp()

  def test_getMaxTimestamp_invalidResult_raises(self):
    MOCK_QUERY.return_value = self.create_table_list_of([100, 200])

    with self.assertRaisesRegex(ValidationError, "Failed validating 'maxItems' in schema"):
      with BucketClient(self.SOURCE_BUCKET_INFO) as client:
        client.get_max_timestamp()

  def test_getMaxTimestamp_nonEmptyBucket_returnsTimestamp(self):
    MOCK_QUERY.return_value = self.create_table_list_of([100])

    with BucketClient(self.SOURCE_BUCKET_INFO) as client:
      self.assertEqual(client.get_max_timestamp(), Timestamp(100))

  def test_copyToBucket_dryRun_doesNotSendToTarget(self):
    MOCK_QUERY.return_value = self.create_table_list_of([5])

    with BucketClient(self.SOURCE_BUCKET_INFO) as client:
      client.copy_to_bucket(self.TARGET_BUCKET_INFO, TimestampRange.ETERNITY)

    MOCK_QUERY.assert_called_once_with(
        'from(bucket: "source-bucket")'
        ' |> range(start: 1677-09-21T00:12:43.145224193Z, stop: 2262-04-11T23:47:16.854775807Z)'
        ' |> count()'
        ' |> group()'
        ' |> sum()'
        ' |> yield()')

  @flagsaver.as_parsed((_DRY_RUN, 'false'))
  def test_copyToBucket_wetRun_sendsToTarget(self):
    MOCK_QUERY.return_value = self.create_table_list_of([5])

    with BucketClient(self.SOURCE_BUCKET_INFO) as client:
      client.copy_to_bucket(self.TARGET_BUCKET_INFO, TimestampRange.ETERNITY)

    MOCK_QUERY.assert_called_once_with(
        'from(bucket: "source-bucket")'
        ' |> range(start: 1677-09-21T00:12:43.145224193Z, stop: 2262-04-11T23:47:16.854775807Z)'
        ' |> to(host: "target-server-url", org: "target-org", bucket: "target-bucket", token: "target-token")'
        ' |> count()'
        ' |> group()'
        ' |> sum()'
        ' |> yield()')

  def test_copyToBucket_emptyRange_returns0(self):
    MOCK_QUERY.return_value = self.create_table_list_of([])

    with BucketClient(self.SOURCE_BUCKET_INFO) as client:
      result = client.copy_to_bucket(self.TARGET_BUCKET_INFO, TimestampRange.ETERNITY)

    self.assertEqual(result, 0)

  def test_copyToBucket_nonEmptyRange_returnsRecordsCopied(self):
    MOCK_QUERY.return_value = self.create_table_list_of([5])

    with BucketClient(self.SOURCE_BUCKET_INFO) as client:
      result = client.copy_to_bucket(self.TARGET_BUCKET_INFO, TimestampRange.ETERNITY)

    self.assertEqual(result, 5)

  def test_copyToBucket_invalidResult_raises(self):
    MOCK_QUERY.return_value = self.create_table_list_of([100, 200])

    with self.assertRaisesRegex(ValidationError, "Failed validating 'maxItems' in schema"):
      with BucketClient(self.SOURCE_BUCKET_INFO) as client:
        client.copy_to_bucket(self.TARGET_BUCKET_INFO, TimestampRange.ETERNITY)

  @flagsaver.as_parsed((_FLUX_QUERY, ['query1()', 'query2()']))
  def test_copyToBucket_customQueryLines_dryRun_addedToQuery(self):
    with BucketClient(self.SOURCE_BUCKET_INFO) as client:
      client.copy_to_bucket(self.TARGET_BUCKET_INFO, TimestampRange.ETERNITY)

    MOCK_QUERY.assert_called_once_with(
        'from(bucket: "source-bucket")'
        ' |> range(start: 1677-09-21T00:12:43.145224193Z, stop: 2262-04-11T23:47:16.854775807Z)'
        ' |> query1()'
        ' |> query2()'
        ' |> count()'
        ' |> group()'
        ' |> sum()'
        ' |> yield()')

  @flagsaver.as_parsed((_DRY_RUN, 'false'), (_FLUX_QUERY, ['query1()', 'query2()']))
  def test_copyToBucket_customQueryLines_wetRun_addedToQuery(self):
    with BucketClient(self.SOURCE_BUCKET_INFO) as client:
      client.copy_to_bucket(self.TARGET_BUCKET_INFO, TimestampRange.ETERNITY)

    MOCK_QUERY.assert_called_once_with(
        'from(bucket: "source-bucket")'
        ' |> range(start: 1677-09-21T00:12:43.145224193Z, stop: 2262-04-11T23:47:16.854775807Z)'
        ' |> query1()'
        ' |> query2()'
        ' |> to(host: "target-server-url", org: "target-org", bucket: "target-bucket", token: "target-token")'
        ' |> count()'
        ' |> group()'
        ' |> sum()'
        ' |> yield()')
