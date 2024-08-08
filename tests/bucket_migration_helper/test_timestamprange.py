from absl.testing import parameterized

from bucket_migration_helper.duration import Duration
from bucket_migration_helper.timestamp import Timestamp
from bucket_migration_helper.timestamprange import TimestampRange


class TestTimestampRange(parameterized.TestCase):

  def test_eternity(self):
    self.assertEqual(TimestampRange.ETERNITY.start, Timestamp.MIN)
    self.assertEqual(TimestampRange.ETERNITY.stop, Timestamp.MAX)

  def test_storesValue(self):
    ts_range = TimestampRange(start=Timestamp(1000), stop=Timestamp(2000))

    self.assertEqual(ts_range.start, Timestamp(1000))
    self.assertEqual(ts_range.stop, Timestamp(2000))

  def test_startEqualsStop_raises(self):
    with self.assertRaises(ValueError):
      TimestampRange(start=Timestamp(1000), stop=Timestamp(1000))

  def test_startBiggerThanStop_raises(self):
    with self.assertRaises(ValueError):
      TimestampRange(start=Timestamp(1001), stop=Timestamp(1000))

  def test_str(self):
    self.assertEqual('range(start: 1677-09-21T00:12:43.145224193Z, stop: 2262-04-11T23:47:16.854775807Z)',
                     str(TimestampRange(start=Timestamp.MIN, stop=Timestamp.MAX)))

  @parameterized.parameters(
      (Timestamp.MIN, Timestamp.ZERO, Duration(9223372036854775807)),
      (Timestamp.ZERO, Timestamp.MAX, Duration(9223372036854775807)),
      (Timestamp.MIN, Timestamp.MAX, Duration(9223372036854775807 * 2)),
  )
  def test_duraton(self, timestamp_1: Timestamp, timestamp_2: Timestamp, resulting_duration):
    self.assertEqual(TimestampRange(timestamp_1, timestamp_2).duraton(), resulting_duration)
