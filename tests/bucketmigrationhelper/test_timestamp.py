from absl.testing import parameterized

from line_protocol_cache.bucketmigrationhelper.timestamp import Timestamp


class TestTimestamp(parameterized.TestCase):
  MIN = -9223372036854775807
  MAX = 9223372036854775807

  @parameterized.parameters(
      (MIN - 1, AssertionError),
      (MAX + 1, AssertionError),
  )
  def test_invalidValue(self, t: int, expected_exception):
    with self.assertRaises(expected_exception):
      Timestamp(t)

  @parameterized.parameters(
      (MIN, '1677-09-21T00:12:43.145224193Z'),
      (MAX, '2262-04-11T23:47:16.854775807Z'),
      (0, '1970-01-01T00:00:00.000000000Z'),
  )
  def test_str(self, t: int, rfc3339: str):
    self.assertEqual(str(Timestamp(t)), rfc3339)

  @parameterized.product(
      t1=[MIN, 0, MAX],
      t2=[MIN, 0, MAX],
  )
  def test_equal(self, t1: int, t2: int):
    if t1 == t2:
      self.assertEqual(Timestamp(t1), Timestamp(t2))
      self.assertEqual(Timestamp(t1), t2)
      self.assertEqual(t1, Timestamp(t2))
    else:
      self.assertNotEqual(Timestamp(t1), Timestamp(t2))
      self.assertNotEqual(Timestamp(t1), t2)
      self.assertNotEqual(t1, Timestamp(t2))

  @parameterized.parameters(
      (MAX, MAX - MIN),
      (0, MAX),
      (MIN + 1, 1),
  )
  def test_subtract(self, t: int, amount: int):
    self.assertEqual(Timestamp(t) - amount, Timestamp(t - amount))
    self.assertEqual(Timestamp(t) - amount, t - amount)

  @parameterized.parameters(
      (MIN, MAX - MIN),
      (0, MAX),
      (MAX - 1, 1),
  )
  def test_add(self, t: int, amount: int):
    self.assertEqual(Timestamp(t) + amount, Timestamp(t + amount))
    self.assertEqual(Timestamp(t) + amount, t + amount)

  @parameterized.parameters(
      (MIN, MAX),
      (MIN, 0),
  )
  def test_less(self, t1: int, t2: int):
    self.assertLess(Timestamp(t1), Timestamp(t2))

  @parameterized.parameters(
      (MAX, MIN),
      (MAX, 0),
  )
  def test_greater(self, t1: int, t2: int):
    self.assertGreater(Timestamp(t1), Timestamp(t2))
