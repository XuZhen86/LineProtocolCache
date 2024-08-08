from absl.testing import parameterized

from bucket_migration_helper.duration import Duration
from bucket_migration_helper.timestamp import Timestamp
from bucket_migration_helper.timestamprange import TimestampRange
from bucket_migration_helper.timestamprangeiterator import TimestampRangeIterator


class TestTimestampRangeIterator(parameterized.TestCase):

  def test_stepNsEquals0_raises(self):
    with self.assertRaises(ValueError):
      TimestampRangeIterator(TimestampRange.ETERNITY, Duration.ZERO)

  def test_stepNsLessThan0_raises(self):
    with self.assertRaises(ValueError):
      TimestampRangeIterator(TimestampRange.ETERNITY, Duration(-1))

  def test_next_partialFinalIteration(self):
    tsi = TimestampRangeIterator(TimestampRange(Timestamp(0), Timestamp(25)), Duration(10))
    self.assertListEqual(list(tsi), [
        TimestampRange(Timestamp(0), Timestamp(10)),
        TimestampRange(Timestamp(10), Timestamp(20)),
        TimestampRange(Timestamp(20), Timestamp(25))
    ])

  def test_next_fullFinalIteration(self):
    tsi = TimestampRangeIterator(TimestampRange(Timestamp(0), Timestamp(30)), Duration(10))
    self.assertListEqual(list(tsi), [
        TimestampRange(Timestamp(0), Timestamp(10)),
        TimestampRange(Timestamp(10), Timestamp(20)),
        TimestampRange(Timestamp(20), Timestamp(30))
    ])

  def test_next_capsAtMax(self):
    tsi = TimestampRangeIterator(TimestampRange(Timestamp.MAX - Duration(10), Timestamp.MAX), Duration(6))
    self.assertEqual(list(tsi), [
        TimestampRange(Timestamp.MAX - Duration(10), Timestamp.MAX - Duration(4)),
        TimestampRange(Timestamp.MAX - Duration(4), Timestamp.MAX)
    ])

  def test_next_capsAtStop(self):
    tsi = TimestampRangeIterator(TimestampRange(Timestamp.MAX - Duration(10), Timestamp.MAX - Duration(2)), Duration(6))
    self.assertEqual(list(tsi), [
        TimestampRange(Timestamp.MAX - Duration(10), Timestamp.MAX - Duration(4)),
        TimestampRange(Timestamp.MAX - Duration(4), Timestamp.MAX - Duration(2))
    ])

  def test_next_oneIteration(self):
    tsi = TimestampRangeIterator(TimestampRange(Timestamp(0), Timestamp(5)), Duration(10))
    self.assertListEqual(list(tsi), [TimestampRange(Timestamp(0), Timestamp(5))])

  def test_length(self):
    tsi = TimestampRangeIterator(TimestampRange.ETERNITY, Duration(3))
    self.assertEqual(tsi.length(), 6148914691236516864)
