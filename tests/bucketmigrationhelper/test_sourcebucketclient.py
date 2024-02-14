from unittest.mock import Mock, patch

from absl import logging
from absl.logging.converter import absl_to_standard
from absl.testing import absltest, flagsaver
from influxdb_client import InfluxDBClient, QueryApi
from influxdb_client.client.flux_table import FluxRecord, FluxTable, TableList

from line_protocol_cache.bucketmigrationhelper.sourcebucketclient import (_MAX_ATTEMPTS, _RETRY_INTERVAL_S, _SRC_BUCKET,
                                                                          _SRC_ORG, _SRC_SERVER_URL, _SRC_TOKEN,
                                                                          SourceBucketClient)
from line_protocol_cache.bucketmigrationhelper.timestamp import Timestamp

MOCK_QUERY_API = Mock(spec=QueryApi)


@patch.object(InfluxDBClient, InfluxDBClient.query_api.__name__, Mock(return_value=MOCK_QUERY_API))
class TestSourceBucketClient(absltest.TestCase):

  def setUp(self):
    self.saved_flags = flagsaver.as_parsed(
        (_SRC_SERVER_URL, 'url'),
        (_SRC_BUCKET, 'bucket'),
        (_SRC_TOKEN, 'token'),
        (_SRC_ORG, 'org'),
    )
    self.saved_flags.__enter__()
    return super().setUp()

  def tearDown(self) -> None:
    self.saved_flags.__exit__(None, None, None)
    MOCK_QUERY_API.reset_mock()
    return super().tearDown()

  @staticmethod
  def tableListOf(only_int_value: int) -> TableList:
    record = FluxRecord('record')
    record['_value'] = only_int_value

    table = FluxTable()
    assert isinstance(table.records, list)
    table.records.append(record)

    table_list = TableList()
    table_list.append(table)
    return table_list

  @patch.object(InfluxDBClient, InfluxDBClient.close.__name__, Mock())
  def test_callsCloseWhenExit(self):
    with SourceBucketClient():
      pass

    InfluxDBClient.close.assert_called_once()

  @patch.object(MOCK_QUERY_API, QueryApi.query.__name__, Mock(return_value=tableListOf(0)))
  def test_execute_success(self):
    query_lines = ['function1()', 'function2()']

    with SourceBucketClient() as client:
      table = client.execute(Timestamp.min(), Timestamp.max(), query_lines)
      value = client.only_int_value(table, 1)

    self.assertEqual(value, 0)
    MOCK_QUERY_API.query.assert_called_once()

  @patch.object(MOCK_QUERY_API, QueryApi.query.__name__, Mock(side_effect=[Exception(), Exception(), tableListOf(0)]))
  @flagsaver.as_parsed((_MAX_ATTEMPTS, str(3)), (_RETRY_INTERVAL_S, str(0.0)))
  def test_execute_retriesOnExceptions(self):
    with self.assertLogs(logger='absl', level=absl_to_standard(logging.WARNING)), SourceBucketClient() as client:
      table = client.execute(Timestamp.min(), Timestamp.max())
      value = client.only_int_value(table, 1)

    self.assertEqual(value, 0)
    self.assertEqual(MOCK_QUERY_API.query.call_count, 3)

  @patch.object(MOCK_QUERY_API, QueryApi.query.__name__, Mock(side_effect=[TimeoutError(), ValueError()]))
  @flagsaver.as_parsed((_MAX_ATTEMPTS, str(2)), (_RETRY_INTERVAL_S, str(0.0)))
  def test_execute_reraisesLastException(self):
    with self.assertLogs(logger='absl', level=absl_to_standard(
        logging.WARNING)), self.assertRaises(ValueError), SourceBucketClient() as client:
      client.execute(Timestamp.min(), Timestamp.max())

    self.assertEqual(MOCK_QUERY_API.query.call_count, 2)

  @patch.object(MOCK_QUERY_API, QueryApi.query.__name__, Mock(return_value=tableListOf(0)))
  def test_execute_insertsQueryLines(self):
    query_lines = ['function1()', 'function2()']

    with SourceBucketClient() as client:
      client.execute(Timestamp.min(), Timestamp.max(), query_lines)

    MOCK_QUERY_API.query.assert_called_once()
    query = MOCK_QUERY_API.query.call_args.args[0]
    self.assertIn(query_lines[0], query)
    self.assertIn(query_lines[1], query)

  @patch.object(MOCK_QUERY_API, QueryApi.query.__name__, Mock(return_value=tableListOf(0)))
  def test_execute_ignoresNoneQueryLines(self):
    query_lines: list[str | None] = ['function1()', None, 'function2()']

    with SourceBucketClient() as client:
      client.execute(Timestamp.min(), Timestamp.max(), query_lines)

    MOCK_QUERY_API.query.assert_called_once()
    query = MOCK_QUERY_API.query.call_args.args[0]
    self.assertIn(query_lines[0], query)
    self.assertIn(query_lines[2], query)

  @patch.object(MOCK_QUERY_API, QueryApi.query.__name__, Mock(return_value=[]))
  def test_assertNonEmptyBucket_raisesOnEmptyBucket(self):
    with self.assertRaises(ValueError), SourceBucketClient() as client:
      client.assert_non_empty_bucket()

    MOCK_QUERY_API.query.assert_called_once()

  @patch.object(MOCK_QUERY_API, QueryApi.query.__name__, Mock(return_value=tableListOf(0)))
  def test_assertNonEmptyBucket_doesNotRaiseOnNonEmptyBucket(self):
    with SourceBucketClient() as client:
      client.assert_non_empty_bucket()

    MOCK_QUERY_API.query.assert_called_once()

  @patch.object(MOCK_QUERY_API, QueryApi.query.__name__, Mock(return_value=tableListOf(0)))
  def test_minTimestamp_returnsTimestamp(self):
    with SourceBucketClient() as client:
      timestamp = client.min_timestamp()

    self.assertEqual(timestamp, Timestamp(0))

  @patch.object(MOCK_QUERY_API, QueryApi.query.__name__, Mock(return_value=[]))
  def test_minTimestamp_defaultsToMinTimestamp(self):
    with SourceBucketClient() as client:
      timestamp = client.min_timestamp()

    self.assertEqual(timestamp, Timestamp.min())

  @patch.object(MOCK_QUERY_API, QueryApi.query.__name__, Mock(return_value=tableListOf(0)))
  def test_maxTimestamp_returnsTimestampPlus1(self):
    with SourceBucketClient() as client:
      timestamp = client.max_timestamp()

    self.assertEqual(timestamp, Timestamp(1))

  @patch.object(MOCK_QUERY_API, QueryApi.query.__name__, Mock(return_value=[]))
  def test_maxTimestamp_defaultsToMaxTimestamp(self):
    with SourceBucketClient() as client:
      timestamp = client.max_timestamp()

    self.assertEqual(timestamp, Timestamp.max())
