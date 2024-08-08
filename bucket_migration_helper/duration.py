import re
from dataclasses import dataclass
from typing import ClassVar, Self


@dataclass(frozen=True, order=True)
class Duration:
  duration_ns: int

  _DURATION_NS_MAX: ClassVar[int] = 0xffff_ffff_ffff_fffe
  _DURATION_NS_MIN: ClassVar[int] = -_DURATION_NS_MAX

  def __post_init__(self) -> None:
    if not self._DURATION_NS_MIN <= self.duration_ns <= self._DURATION_NS_MAX:
      raise ValueError(f'value {self.duration_ns} out of range, '
                       f'expected to be in range [{self._DURATION_NS_MIN}, {self._DURATION_NS_MAX}]')

  _SECOND_NS: ClassVar[int] = 10**9
  _MINUTE_NS: ClassVar[int] = _SECOND_NS * 60
  _HOUR_NS: ClassVar[int] = _MINUTE_NS * 60

  def __str__(self) -> str:
    ns = self.duration_ns
    sign = '-' if ns < 0 else '+'
    ns = abs(ns)

    hours = ns // self._HOUR_NS
    ns %= self._HOUR_NS
    minutes = ns // self._MINUTE_NS
    ns %= self._MINUTE_NS
    seconds = ns // self._SECOND_NS
    ns %= self._SECOND_NS

    return f'{sign}{hours:02d}:{minutes:02d}:{seconds:02d}.{ns:09d}'

  def __truediv__(self, other: object) -> float:
    if isinstance(other, Duration):
      return self.duration_ns / other.duration_ns
    return NotImplemented

  def __mul__(self, other: object) -> Self:
    if isinstance(other, int):
      return self.__class__(self.duration_ns * other)
    return NotImplemented

  _REGEX: ClassVar[str] = (r'^'
                           r'(?P<sign>[+-])'
                           r'(?P<hours>\d{2,})'
                           r':(?P<minutes>\d{2,})'
                           r':(?P<seconds>\d{2,})'
                           r'\.(?P<nanoseconds>\d{9,})'
                           r'$')
  _PATTERN: ClassVar[re.Pattern[str]] = re.compile(_REGEX)

  @classmethod
  def build(cls, s: str) -> Self:
    try:
      return cls(int(s))
    except ValueError:
      pass

    assert (match := cls._PATTERN.search(s)) is not None, f'unable to match regex {cls._REGEX}'
    hours = int(match['hours'])
    minutes = int(match['minutes'])
    seconds = int(match['seconds'])
    nanoseconds = int(match['nanoseconds'])

    duration_ns = hours * cls._HOUR_NS
    duration_ns += minutes * cls._MINUTE_NS
    duration_ns += seconds * cls._SECOND_NS
    duration_ns += nanoseconds
    duration_ns *= (-1 if match['sign'] == '-' else 1)

    return cls(duration_ns)

  MAX: ClassVar[Self]
  MIN: ClassVar[Self]
  ZERO: ClassVar[Self]
  HOUR: ClassVar[Self]


Duration.MAX = Duration(Duration._DURATION_NS_MAX)
Duration.MIN = Duration(Duration._DURATION_NS_MIN)
Duration.ZERO = Duration(0)
Duration.HOUR = Duration(Duration._HOUR_NS)
