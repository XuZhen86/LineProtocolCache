import os
import sqlite3
import tempfile

from absl import logging
from absl.testing import absltest
from influxdb_client import Point

from line_protocol_cache.lineprotocolcache import LineProtocolCache, LineProtocolCacheConfig


class TestLineProtocolCache(absltest.TestCase):
  SELECT_ROWS = 'SELECT line_protocol FROM LineProtocolCache;'

  POINT_1 = Point('m').tag('t', 0).field('f', 1)
  POINT_2 = Point('m').tag('t', 0).field('f', 2)
  POINTS_1 = [Point('m').tag('t', 1).field('f', i) for i in range(10)]
  POINTS_2 = [Point('m').tag('t', 2).field('f', i) for i in range(10)]
  ALL_POINTS = [POINT_1, POINT_2] + POINTS_1 + POINTS_2

  def setUp(self):
    self.temp_dir = tempfile.TemporaryDirectory()
    self.config = LineProtocolCacheConfig(
        cache_path=os.path.join(self.temp_dir.name, 'cache.sql'),
        sample_interval_s=0,
        batch_size=4,
    )
    return super().setUp()

  def tearDown(self):
    self.temp_dir.cleanup()
    return super().tearDown()

  def test_putPoints_nonEmptyDatabase(self):
    with LineProtocolCache(self.config):
      LineProtocolCache.put(self.POINT_1, self.POINT_2, self.POINTS_1, self.POINTS_2)

    with sqlite3.connect(self.config.cache_path) as connection, connection:
      rows = connection.execute(self.SELECT_ROWS).fetchall()
    self.assertListEqual([row[0] for row in rows], [point.to_line_protocol() for point in self.ALL_POINTS])

  def test_noPoints_emptyDatabase(self):
    with LineProtocolCache(self.config):
      pass

    with sqlite3.connect(self.config.cache_path) as connection, connection:
      rows = connection.execute(self.SELECT_ROWS).fetchall()
    self.assertEmpty(rows)

  def test_queueNotOpen_raises(self):
    with self.assertRaises(ValueError):
      LineProtocolCache.put(self.POINT_1)

  def test_sampleInterval0_logsAllPoints(self):
    with self.assertLogs(logger='absl', level=logging.INFO) as logs:
      with LineProtocolCache(self.config):
        LineProtocolCache.put(self.POINT_1, self.POINT_2, self.POINTS_1, self.POINTS_2)

    self.assertContainsExactSubsequence(
        [record.message for record in logs.records],
        [point.to_line_protocol() for point in self.ALL_POINTS],
    )
