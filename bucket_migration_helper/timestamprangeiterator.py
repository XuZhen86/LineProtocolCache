import math
from typing import Iterator, Self

from .duration import Duration
from .timestamprange import TimestampRange


class TimestampRangeIterator(Iterator[TimestampRange]):

  def __init__(self, ts_range: TimestampRange, step: Duration) -> None:
    if step <= Duration.ZERO:
      raise ValueError(f'expected step to be a positive Duration, got {step}')

    self.ts_range = ts_range
    self.step = step
    self._i = 0
    self._n = math.ceil(self.ts_range.duraton() / self.step)

  def __iter__(self) -> Self:
    return self

  def __next__(self) -> TimestampRange:
    if self._i == self._n:
      raise StopIteration()

    start = self.ts_range.start + self.step * self._i
    try:
      stop = min(start + self.step, self.ts_range.stop)
    except ValueError:
      stop = self.ts_range.stop

    self._i += 1
    return TimestampRange(start, stop)

  def length(self) -> int:
    return self._n
