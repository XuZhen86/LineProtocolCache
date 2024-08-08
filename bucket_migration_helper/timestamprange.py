from dataclasses import dataclass
from typing import ClassVar, Self

from .duration import Duration
from .timestamp import Timestamp


@dataclass(frozen=True)
class TimestampRange:
  ETERNITY: ClassVar[Self]

  start: Timestamp
  stop: Timestamp

  def __post_init__(self) -> None:
    if not self.start < self.stop:
      raise ValueError(f'start timestamp {self.start} must be smaller than stop timestamp {self.stop}')

  def __str__(self) -> str:
    return f'range(start: {self.start}, stop: {self.stop})'

  def duraton(self) -> Duration:
    return self.stop - self.start


TimestampRange.ETERNITY = TimestampRange(start=Timestamp.MIN, stop=Timestamp.MAX)
