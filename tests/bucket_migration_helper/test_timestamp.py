from absl.testing import parameterized

from bucket_migration_helper.duration import Duration
from bucket_migration_helper.timestamp import Timestamp


class TestTimestamp(parameterized.TestCase):
  MIN = -9223372036854775807
  MAX = 9223372036854775807

  @parameterized.parameters(
      (MIN - 1, ValueError),
      (MAX + 1, ValueError),
  )
  def test_invalidValue(self, instant_ns: int, expected_exception: type[Exception]):
    with self.assertRaises(expected_exception):
      Timestamp(instant_ns)

  @parameterized.parameters(
      ('1677-09-21T00:12:43.145224193Z', Timestamp.MIN),
      ('2262-04-11T23:47:16.854775807Z', Timestamp.MAX),
      ('1970-01-01T00:00:00.000000000Z', Timestamp.ZERO),
      ('1970-01-01T00:00:00.000000001Z', Timestamp(1)),
      ('1969-12-31T23:59:59.999999999Z', Timestamp(-1)),
      ('1970-01-01T00:00:01.000000000Z', Timestamp(10**9)),
      ('1969-12-31T23:59:59.000000000Z', Timestamp(-10**9)),
      ('1970-01-01T00:01:00.000000000Z', Timestamp(60 * 10**9)),
      ('1969-12-31T23:59:00.000000000Z', Timestamp(60 * -10**9)),
      ('1970-01-01T01:00:00.000000000Z', Timestamp(60 * 60 * 10**9)),
      ('1969-12-31T23:00:00.000000000Z', Timestamp(60 * 60 * -10**9)),
      ('1970-01-02T00:00:00.000000000Z', Timestamp(24 * 60 * 60 * 10**9)),
      ('1969-12-31T00:00:00.000000000Z', Timestamp(24 * 60 * 60 * -10**9)),
      ('1970-01-08T00:00:00.000000000Z', Timestamp(7 * 24 * 60 * 60 * 10**9)),
      ('1969-12-25T00:00:00.000000000Z', Timestamp(7 * 24 * 60 * 60 * -10**9)),
  )
  def test_str(self, rfc3339: str, timestamp: Timestamp):
    self.assertEqual(str(timestamp), rfc3339)

  @parameterized.product(
      instant_ns_1=[MIN, 0, MAX],
      instant_ns_2=[MIN, 0, MAX],
  )
  def test_equal(self, instant_ns_1: int, instant_ns_2: int):
    if instant_ns_1 == instant_ns_2:
      self.assertEqual(Timestamp(instant_ns_1), Timestamp(instant_ns_2))
      self.assertEqual(Timestamp(instant_ns_1), instant_ns_2)
      self.assertEqual(instant_ns_1, Timestamp(instant_ns_2))
    else:
      self.assertNotEqual(Timestamp(instant_ns_1), Timestamp(instant_ns_2))
      self.assertNotEqual(Timestamp(instant_ns_1), instant_ns_2)
      self.assertNotEqual(instant_ns_1, Timestamp(instant_ns_2))

  @parameterized.parameters(
      (Timestamp.MAX, Duration(abs(MIN - MAX)), Timestamp.MIN),
      (Timestamp.ZERO, Duration.ZERO, Timestamp.ZERO),
      (Timestamp.ZERO, Duration(abs(MIN)), Timestamp.MIN),
  )
  def test_subtract_duration(self, timestamp: Timestamp, duration: Duration, resulting_timestamp: Timestamp):
    self.assertEqual(timestamp - duration, resulting_timestamp)

  @parameterized.product(
      timestamp_1=[Timestamp.MIN, Timestamp.ZERO, Timestamp.MAX],
      timestamp_2=[Timestamp.MIN, Timestamp.ZERO, Timestamp.MAX],
  )
  def test_subtract_timestamp(self, timestamp_1: Timestamp, timestamp_2: Timestamp):
    self.assertEqual(timestamp_1 - timestamp_2, Duration(timestamp_1.instant_ns - timestamp_2.instant_ns))

  @parameterized.parameters(
      (Timestamp.MIN, Duration(abs(MAX - MIN)), Timestamp.MAX),
      (Timestamp.ZERO, Duration.ZERO, Timestamp.ZERO),
      (Timestamp.ZERO, Duration(abs(MAX)), Timestamp.MAX),
  )
  def test_add(self, timestamp: Timestamp, duration: Duration, resulting_timestamp: Timestamp):
    self.assertEqual(timestamp + duration, resulting_timestamp)

  @parameterized.parameters(
      (Timestamp.MIN, Timestamp.ZERO),
      (Timestamp.ZERO, Timestamp.MAX),
      (Timestamp.MIN, Timestamp.MAX),
  )
  def test_less(self, timestamp_1: Timestamp, timestamp_2: Timestamp):
    self.assertLess(timestamp_1, timestamp_2)

  @parameterized.parameters(
      (Timestamp.MIN, Timestamp.ZERO),
      (Timestamp.ZERO, Timestamp.MAX),
      (Timestamp.MIN, Timestamp.MAX),
      (Timestamp.MIN, Timestamp.MIN),
      (Timestamp.MAX, Timestamp.MAX),
      (Timestamp.ZERO, Timestamp.ZERO),
  )
  def test_lessEqual(self, timestamp_1: Timestamp, timestamp_2: Timestamp):
    self.assertLessEqual(timestamp_1, timestamp_2)

  @parameterized.parameters(
      (Timestamp.ZERO, Timestamp.MIN),
      (Timestamp.MAX, Timestamp.ZERO),
      (Timestamp.MAX, Timestamp.MIN),
  )
  def test_greater(self, timestamp_1: Timestamp, timestamp_2: Timestamp):
    self.assertGreater(timestamp_1, timestamp_2)

  @parameterized.parameters(
      (Timestamp.ZERO, Timestamp.MIN),
      (Timestamp.MAX, Timestamp.ZERO),
      (Timestamp.MAX, Timestamp.MIN),
      (Timestamp.MIN, Timestamp.MIN),
      (Timestamp.MAX, Timestamp.MAX),
      (Timestamp.ZERO, Timestamp.ZERO),
  )
  def test_greaterEqual(self, timestamp_1: Timestamp, timestamp_2: Timestamp):
    self.assertGreaterEqual(timestamp_1, timestamp_2)

  @parameterized.parameters(
      ('9223372036854775807', Timestamp.MAX),
      ('-9223372036854775807', Timestamp.MIN),
      ('0', Timestamp.ZERO),
      ('0000', Timestamp.ZERO),
  )
  def test_build_int(self, s: str, expected_timestamp: Timestamp):
    self.assertEqual(Timestamp.build(s), expected_timestamp)

  @parameterized.parameters(
      ('1677-09-21T00:12:43.145224193Z', Timestamp.MIN),
      ('2262-04-11T23:47:16.854775807Z', Timestamp.MAX),
      ('1970-01-01T00:00:00.000000000Z', Timestamp.ZERO),
      ('1970-01-01T00:00:00.000000001Z', Timestamp(1)),
      ('1969-12-31T23:59:59.999999999Z', Timestamp(-1)),
      ('1970-01-01T00:00:01.000000000Z', Timestamp(10**9)),
      ('1969-12-31T23:59:59.000000000Z', Timestamp(-10**9)),
      ('1970-01-01T00:01:00.000000000Z', Timestamp(60 * 10**9)),
      ('1969-12-31T23:59:00.000000000Z', Timestamp(60 * -10**9)),
      ('1970-01-01T01:00:00.000000000Z', Timestamp(60 * 60 * 10**9)),
      ('1969-12-31T23:00:00.000000000Z', Timestamp(60 * 60 * -10**9)),
      ('1970-01-02T00:00:00.000000000Z', Timestamp(24 * 60 * 60 * 10**9)),
      ('1969-12-31T00:00:00.000000000Z', Timestamp(24 * 60 * 60 * -10**9)),
      ('1970-01-08T00:00:00.000000000Z', Timestamp(7 * 24 * 60 * 60 * 10**9)),
      ('1969-12-25T00:00:00.000000000Z', Timestamp(7 * 24 * 60 * 60 * -10**9)),
  )
  def test_build_regex(self, rfc3339: str, expected_timestamp: Timestamp):
    self.assertEqual(Timestamp.build(rfc3339), expected_timestamp)

  @parameterized.parameters(
      ('1970-01-0100:00:00.000000000Z'),
      ('1970-01-01T00:00:00.000000000'),
      ('197-01-01T00:00:00.000000000Z'),
      ('19700-01-01T00:00:00.000000000Z'),
      ('1970-1-01T00:00:00.000000000Z'),
      ('1970-010-01T00:00:00.000000000Z'),
      ('1970-01-1T00:00:00.000000000Z'),
      ('1970-01-010T00:00:00.000000000Z'),
      ('1970-01-01T0:00:00.000000000Z'),
      ('1970-01-01T001:00:00.000000000Z'),
      ('1970-01-01T00:0:00.000000000Z'),
      ('1970-01-01T00:001:00.000000000Z'),
      ('1970-01-01T00:00:0.000000000Z'),
      ('1970-01-01T00:00:001.000000000Z'),
      ('1970-01-01T00:00:00.00000000Z'),
      ('1970-01-01T00:00:00.0000000001Z'),
      ('1677-09-21T00:12:43.145224192Z'),
      ('2262-04-11T23:47:16.854775808Z'),
      ('0000-01-01T00:00:00.000000000Z'),
      ('9999-01-01T00:00:00.000000000Z'),
  )
  def test_build_invalidString_raises(self, s: str):
    with self.assertRaises(Exception):
      Duration.build(s)
