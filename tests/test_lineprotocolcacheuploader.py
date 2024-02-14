import os
import sqlite3
import tempfile
from threading import Event
from unittest.mock import Mock, call, patch

from absl import logging
from absl.flags import IllegalFlagValueError
from absl.logging.converter import absl_to_standard
from absl.testing import absltest, flagsaver
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import WriteApi

from line_protocol_cache.lineprotocolcacheuploader import (_BATCH_SIZE, _BUCKETS, _CACHE_PATH, _CATCHING_UP_INTERVAL,
                                                           _ORGS, _SAMPLE_INTERVAL, _TOKENS, _UPLOAD_INTERVAL, _URLS,
                                                           LineProtocolCacheUploader)

MOCK_EVENT = Mock(spec=Event)
MOCK_WRITE_API = Mock(spec=WriteApi)


@patch.object(InfluxDBClient, InfluxDBClient.write_api.__name__, Mock(return_value=MOCK_WRITE_API))
class TestLineProtocolCacheUploader(absltest.TestCase):
  CREATE_TABLE = 'CREATE TABLE IF NOT EXISTS LineProtocolCache (line_protocol TEXT NOT NULL);'
  INSERT_ROW = 'INSERT INTO LineProtocolCache (line_protocol) VALUES (?);'
  SELECT_ROWS = 'SELECT line_protocol FROM LineProtocolCache;'

  ROWS = [f'row #{i}' for i in range(10)]

  def setUp(self):
    self.temp_dir = tempfile.TemporaryDirectory()
    self.cache_path = os.path.join(self.temp_dir.name, 'cache.sql')
    self.connection = sqlite3.connect(self.cache_path)
    with self.connection:
      self.connection.execute(self.CREATE_TABLE).arraysize
      self.connection.executemany(self.INSERT_ROW, [(row,) for row in self.ROWS])

    self.saved_flags = flagsaver.as_parsed(
        (_URLS, ['url1', 'url2']),
        (_BUCKETS, ['bucket1', 'bucket2']),
        (_TOKENS, ['token1', 'token2']),
        (_ORGS, ['org1', 'org2']),
        (_CACHE_PATH, self.cache_path),
        (_BATCH_SIZE, str(4)),
    )
    self.saved_flags.__enter__()

    return super().setUp()

  def tearDown(self):
    self.saved_flags.__exit__(None, None, None)
    self.connection.close()
    self.temp_dir.cleanup()
    MOCK_EVENT.reset_mock()
    MOCK_WRITE_API.reset_mock()
    return super().tearDown()

  def test_serverFlagsDifferentLength_raises(self):
    with self.assertRaises(IllegalFlagValueError):
      with flagsaver.as_parsed(
          (_URLS, ['url1']),
          (_BUCKETS, ['bucket1', 'bucket2']),
          (_TOKENS, ['token1', 'token2']),
          (_ORGS, ['org1', 'org2']),
      ):
        pass

  @patch.object(MOCK_EVENT, Event.is_set.__name__, Mock(side_effect=[False, False, False, True]))
  def test_uploadsInOrderAndDeletes(self):
    with LineProtocolCacheUploader() as uploader:
      uploader.run(MOCK_EVENT)

    self.assertListEqual(MOCK_WRITE_API.write.call_args_list, [
        call(bucket='bucket1', record=self.ROWS[:4]),
        call(bucket='bucket2', record=self.ROWS[:4]),
        call(bucket='bucket1', record=self.ROWS[4:8]),
        call(bucket='bucket2', record=self.ROWS[4:8]),
        call(bucket='bucket1', record=self.ROWS[8:]),
        call(bucket='bucket2', record=self.ROWS[8:]),
    ])
    with self.connection:
      self.assertEmpty(self.connection.execute(self.SELECT_ROWS).fetchall())

  @patch.object(MOCK_EVENT, Event.is_set.__name__, Mock(return_value=False))
  @patch.object(MOCK_WRITE_API, WriteApi.write.__name__, Mock(side_effect=[None, Exception()]))
  def test_uploadFails_doesNotDelete(self):

    with self.assertRaises(Exception), LineProtocolCacheUploader() as uploader:
      uploader.run(MOCK_EVENT)

    self.assertListEqual(MOCK_WRITE_API.write.call_args_list, [
        call(bucket='bucket1', record=self.ROWS[:4]),
        call(bucket='bucket2', record=self.ROWS[:4]),
    ])
    with self.connection:
      self.assertListEqual(
          self.connection.execute(self.SELECT_ROWS).fetchall(),
          [(row,) for row in self.ROWS],
      )

  @patch.object(MOCK_EVENT, Event.is_set.__name__, Mock(side_effect=[False, False, False, True]))
  def test_backlogged_catchesUpAndLogs(self):
    with self.assertLogs(logger='absl', level=absl_to_standard(logging.INFO)) as logs:
      with LineProtocolCacheUploader() as uploader:
        uploader.run(MOCK_EVENT)

    self.assertContainsExactSubsequence(
        MOCK_EVENT.wait.call_args_list,
        [
            call(_CATCHING_UP_INTERVAL.default),
            call(_CATCHING_UP_INTERVAL.default),
            call(_UPLOAD_INTERVAL.default),
        ],
    )
    self.assertContainsSubsequence(
        [record.message for record in logs.records],
        ['Catching up, count=10.', 'Catching up, count=6.'],
    )

  @patch.object(MOCK_EVENT, Event.is_set.__name__, Mock(side_effect=[False, False, False, True]))
  @flagsaver.as_parsed((_SAMPLE_INTERVAL, str(0.0)))
  def test_sampleInterval0_logsAllPoints(self):
    with self.assertLogs(logger='absl', level=absl_to_standard(logging.INFO)) as logs:
      with LineProtocolCacheUploader() as uploader:
        uploader.run(MOCK_EVENT)

    self.assertContainsSubsequence([record.message for record in logs.records], self.ROWS)
