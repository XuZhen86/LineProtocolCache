from absl.testing import parameterized

from bucket_migration_helper.duration import Duration


class TestTimestamp(parameterized.TestCase):
  MAX = 0xffff_ffff_ffff_fffe
  MIN = -MAX

  @parameterized.parameters(
      (MIN - 1, ValueError),
      (MAX + 1, ValueError),
  )
  def test_invalidValue(self, duration_ns: int, expected_exception: type[Exception]):
    with self.assertRaises(expected_exception):
      Duration(duration_ns)

  @parameterized.parameters(
      ('-5124095:34:33.709551614', Duration.MIN),
      ('+5124095:34:33.709551614', Duration.MAX),
      ('+00:00:00.000000000', Duration.ZERO),
      ('+00:00:00.000000001', Duration(1)),
      ('-00:00:00.000000001', Duration(-1)),
      ('+00:00:01.000000000', Duration(10**9)),
      ('-00:00:01.000000000', Duration(-10**9)),
      ('+00:01:00.000000000', Duration(60 * 10**9)),
      ('-00:01:00.000000000', Duration(60 * -10**9)),
      ('+01:00:00.000000000', Duration(60 * 60 * 10**9)),
      ('-01:00:00.000000000', Duration(60 * 60 * -10**9)),
      ('+24:00:00.000000000', Duration(24 * 60 * 60 * 10**9)),
      ('-24:00:00.000000000', Duration(24 * 60 * 60 * -10**9)),
      ('+168:00:00.000000000', Duration(7 * 24 * 60 * 60 * 10**9)),
      ('-168:00:00.000000000', Duration(7 * 24 * 60 * 60 * -10**9)),
  )
  def test_str(self, s: str, duration: Duration):
    self.assertEqual(str(duration), s)

  @parameterized.parameters(
      (Duration(1), Duration(1), 1.0),
      (Duration.MAX, Duration.MIN, -1.0),
      (Duration.MIN, Duration.MAX, -1.0),
      (Duration.MAX, Duration(1), 18446744073709551614.0),
      (Duration.MIN, Duration(1), -18446744073709551614.0),
      (Duration.ZERO, Duration(1), 0.0),
  )
  def test_true_division(self, duration_1: Duration, duration_2: Duration, ratio: float):
    self.assertEqual(duration_1 / duration_2, ratio)

  @parameterized.parameters(
      (Duration.ZERO, 1, Duration.ZERO),
      (Duration.MAX, 1, Duration.MAX),
      (Duration.MIN, 1, Duration.MIN),
      (Duration(1), 100, Duration(100)),
      (Duration(-1), 100, Duration(-100)),
  )
  def test_multiplication(self, duration: Duration, ratio: int, expected_duration: Duration):
    self.assertEqual(duration * ratio, expected_duration)

  @parameterized.parameters(
      ('0', Duration.ZERO),
      ('0000', Duration.ZERO),
      ('18446744073709551614', Duration.MAX),
      ('-18446744073709551614', Duration.MIN),
      ('100', Duration(100)),
      ('-100', Duration(-100)),
  )
  def test_build_int(self, s: str, expected_duration: Duration):
    self.assertEqual(Duration.build(s), expected_duration)

  @parameterized.parameters(
      ('-5124095:34:33.709551614', Duration.MIN),
      ('+5124095:34:33.709551614', Duration.MAX),
      ('+00:00:00.000000000', Duration.ZERO),
      ('+00:00:00.000000001', Duration(1)),
      ('-00:00:00.000000001', Duration(-1)),
      ('+00:00:00.1000000000', Duration(10**9)),
      ('+00:00:01.000000000', Duration(10**9)),
      ('-00:00:01.000000000', Duration(-10**9)),
      ('+00:00:60.000000000', Duration(60 * 10**9)),
      ('+00:01:00.000000000', Duration(60 * 10**9)),
      ('-00:01:00.000000000', Duration(60 * -10**9)),
      ('+00:60:00.000000000', Duration(60 * 60 * 10**9)),
      ('+01:00:00.000000000', Duration(60 * 60 * 10**9)),
      ('-01:00:00.000000000', Duration(60 * 60 * -10**9)),
      ('+24:00:00.000000000', Duration(24 * 60 * 60 * 10**9)),
      ('-24:00:00.000000000', Duration(24 * 60 * 60 * -10**9)),
      ('+168:00:00.000000000', Duration(7 * 24 * 60 * 60 * 10**9)),
      ('-168:00:00.000000000', Duration(7 * 24 * 60 * 60 * -10**9)),
  )
  def test_build_regex(self, s: str, expected_duration: Duration):
    self.assertEqual(Duration.build(s), expected_duration)

  @parameterized.parameters(
      ('aaaaa'),
      ('00:00:00.000000000'),
      ('=00:00:00.000000000'),
      ('+:00:00.000000000'),
      ('+0:00:00.000000000'),
      ('+00::00.000000000'),
      ('+00:0:00.000000000'),
      ('+00:00:.000000000'),
      ('+00:00:0.000000000'),
  )
  def test_build_invalidString_raises(self, s: str):
    with self.assertRaises(Exception):
      Duration.build(s)
