import os
import sqlite3
import tempfile
from threading import Event
from unittest.mock import Mock, call, patch

from absl import logging
from absl.testing import absltest, flagsaver
from influxdb_client.client.write_api import WriteApi

from line_protocol_cache.lineprotocolcacheuploader import (_BATCH_SIZE, _BUCKET, _BUCKET_ORG, _BUCKET_TOKEN,
                                                           _CACHE_PATH, _CATCHING_UP_INTERVAL, _HTTP_TIMEOUT,
                                                           _SAMPLE_INTERVAL, _SERVER_URL, _SQLITE_TIMEOUT,
                                                           _UPLOAD_INTERVAL, LineProtocolCacheUploader)


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
        (_SERVER_URL, 'server-url'),
        (_BUCKET, 'bucket'),
        (_BUCKET_TOKEN, 'bucket-token'),
        (_BUCKET_ORG, 'org'),
        (_HTTP_TIMEOUT, str(_HTTP_TIMEOUT.default)),
        (_CACHE_PATH, self.cache_path),
        (_SQLITE_TIMEOUT, str(_SQLITE_TIMEOUT.default)),
        (_UPLOAD_INTERVAL, str(_UPLOAD_INTERVAL.default)),
        (_CATCHING_UP_INTERVAL, str(_CATCHING_UP_INTERVAL.default)),
        (_SAMPLE_INTERVAL, str(_SAMPLE_INTERVAL.default)),
        (_BATCH_SIZE, str(4)),
    )
    self.saved_flags.__enter__()

  def tearDown(self):
    self.saved_flags.__exit__(None, None, None)
    self.connection.close()
    self.temp_dir.cleanup()
    return super().tearDown()

  @patch.object(WriteApi, 'write', Mock())
  @patch.object(Event, 'wait', Mock())
  @patch.object(Event, 'is_set', Mock(side_effect=[False] * 3 + [True]))
  def test_uploadsInOrderAndDeletes(self):
    with LineProtocolCacheUploader() as uploader:
      uploader.run()

    self.assertListEqual(WriteApi.write.call_args_list, [
        call(bucket='bucket', record=self.ROWS[:4]),
        call(bucket='bucket', record=self.ROWS[4:8]),
        call(bucket='bucket', record=self.ROWS[8:]),
    ])
    with self.connection:
      self.assertEmpty(self.connection.execute(self.SELECT_ROWS).fetchall())

  @patch.object(WriteApi, 'write', Mock(side_effect=Exception()))
  @patch.object(Event, 'wait', Mock())
  @patch.object(Event, 'is_set', Mock(return_value=False))
  def test_uploadFaile_doesNotDelete(self):
    with self.assertRaises(Exception), LineProtocolCacheUploader() as uploader:
      uploader.run()

    WriteApi.write.assert_called_once_with(bucket='bucket', record=self.ROWS[:4])
    with self.connection:
      self.assertListEqual(
          self.connection.execute(self.SELECT_ROWS).fetchall(),
          [(row,) for row in self.ROWS],
      )

  @patch.object(WriteApi, 'write', Mock())
  @patch.object(Event, 'wait', Mock())
  @patch.object(Event, 'is_set', Mock(side_effect=[False] * 3 + [True]))
  def test_backlogged_catchesUpAndLogs(self):
    with self.assertLogs(logger='absl', level=logging.INFO) as logs:
      with LineProtocolCacheUploader() as uploader:
        uploader.run()

    self.assertContainsExactSubsequence(
        Event.wait.call_args_list,
        [
            call(_CATCHING_UP_INTERVAL.value),
            call(_CATCHING_UP_INTERVAL.value),
            call(_UPLOAD_INTERVAL.value),
        ],
    )
    self.assertContainsSubsequence(
        [record.message for record in logs.records],
        ['Catching up, count=10.', 'Catching up, count=6.'],
    )

  @patch.object(WriteApi, 'write', Mock())
  @patch.object(Event, 'wait', Mock())
  @patch.object(Event, 'is_set', Mock(side_effect=[False] * 3 + [True]))
  @flagsaver.as_parsed((_SAMPLE_INTERVAL, str(0.0)))
  def test_sampleInterval0_logsAllPoints(self):
    with self.assertLogs(logger='absl', level=logging.INFO) as logs:
      with LineProtocolCacheUploader() as uploader:
        uploader.run()

    self.assertContainsSubsequence([record.message for record in logs.records], self.ROWS)
