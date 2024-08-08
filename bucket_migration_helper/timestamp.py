import re
from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar, Self, overload

from .duration import Duration


@dataclass(frozen=True, order=True)
class Timestamp:
  instant_ns: int

  # https://docs.influxdata.com/influxdb/v1/troubleshooting/frequently-asked-questions/#what-are-the-minimum-and-maximum-timestamps-that-influxdb-can-store
  # Officially it's 2262-04-11T23:47:16.854775806Z, 9223372036854775806, 0x7fff_ffff_ffff_fffe
  # range() accepts 2262-04-11T23:47:16.854775807Z, 9223372036854775807, 0x7fff_ffff_ffff_ffff
  _NANOSECONDS_MAX: ClassVar[int] = 9223372036854775807
  # Officially it's 1677-09-21T00:12:43.145224194Z, -9223372036854775806, 0x8000_0000_0000_0002
  # range() accepts 1677-09-21T00:12:43.145224193Z, -9223372036854775807, 0x8000_0000_0000_0001
  _NANOSECONDS_MIN: ClassVar[int] = -9223372036854775807

  def __post_init__(self) -> None:
    if not self._NANOSECONDS_MIN <= self.instant_ns <= self._NANOSECONDS_MAX:
      raise ValueError(f'value {self.instant_ns} out of range, '
                       f'expected to be in range [{self._NANOSECONDS_MIN}, {self._NANOSECONDS_MAX}]')

  def __str__(self) -> str:
    iso_format = datetime.fromtimestamp(self.instant_ns // 10**9).isoformat()
    nanoseconds = '{:09d}'.format(self.instant_ns % 10**9)
    return iso_format + '.' + nanoseconds + 'Z'

  @overload
  def __sub__(self, other: Duration) -> Self:
    ...

  @overload
  def __sub__(self, other: Self) -> Duration:
    ...

  def __sub__(self, other: object) -> Self | Duration:
    if isinstance(other, Duration):
      return self.__class__(self.instant_ns - other.duration_ns)
    if isinstance(other, Timestamp):
      return Duration(self.instant_ns - other.instant_ns)
    return NotImplemented

  def __add__(self, other: object) -> Self:
    if isinstance(other, Duration):
      return self.__class__(self.instant_ns + other.duration_ns)
    return NotImplemented

  def __eq__(self, other: object) -> bool:
    if isinstance(other, int):
      return self.instant_ns == other
    if isinstance(other, Timestamp):
      return self.instant_ns == other.instant_ns
    return NotImplemented

  def __index__(self) -> int:
    return self.instant_ns

  _REGEX: ClassVar[str] = (r'^'
                           r'(?P<date>\d{4}-\d{2}-?\d{2}T?\d{2}:\d{2}:\d{2})'
                           r'\.(?P<nanoseconds>\d{9})Z'
                           r'$')
  _PATTERN: ClassVar[re.Pattern[str]] = re.compile(_REGEX)
  _STRPTIME_FORMAT: ClassVar[str] = '%Y-%m-%dT%H:%M:%S'
  _EPOCH_DATE: ClassVar[datetime] = datetime(1970, 1, 1)

  @classmethod
  def build(cls, s: str) -> Self:
    try:
      return cls(int(s))
    except ValueError:
      pass

    assert (match := cls._PATTERN.search(s)) is not None, f'unable to match regex {cls._REGEX}'
    date = datetime.strptime(str(match['date']), cls._STRPTIME_FORMAT)
    nanoseconds = int(match['nanoseconds'])

    seconds = int((date - cls._EPOCH_DATE).total_seconds())

    instant_ns = seconds * 10**9 + nanoseconds
    return cls(instant_ns)

  MAX: ClassVar[Self]
  MIN: ClassVar[Self]
  ZERO: ClassVar[Self]


Timestamp.MAX = Timestamp(Timestamp._NANOSECONDS_MAX)
Timestamp.MIN = Timestamp(Timestamp._NANOSECONDS_MIN)
Timestamp.ZERO = Timestamp(0)
