from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar, Self


@dataclass(frozen=True, order=True)
class Timestamp:
  # https://docs.influxdata.com/influxdb/v1/troubleshooting/frequently-asked-questions/#what-are-the-minimum-and-maximum-timestamps-that-influxdb-can-store
  # Officially it's 2262-04-11T23:47:16.854775806Z, 9223372036854775806, 0x7fff_ffff_ffff_fffe
  # range() accepts 2262-04-11T23:47:16.854775807Z, 9223372036854775807, 0x7fff_ffff_ffff_ffff
  _NANOSECONDS_MAX: ClassVar[int] = 9223372036854775807
  # Officially it's 1677-09-21T00:12:43.145224194Z, -9223372036854775806, 0x8000_0000_0000_0002
  # range() accepts 1677-09-21T00:12:43.145224193Z, -9223372036854775807, 0x8000_0000_0000_0001
  _NANOSECONDS_MIN: ClassVar[int] = -9223372036854775807

  nanoseconds: int

  def __post_init__(self) -> None:
    assert self._NANOSECONDS_MIN <= self.nanoseconds <= self._NANOSECONDS_MAX

  def __str__(self) -> str:
    iso_format = datetime.fromtimestamp(self.nanoseconds // 10**9).isoformat()
    nano_seconds = '{:09d}'.format(self.nanoseconds % 10**9)
    return iso_format + '.' + nano_seconds + 'Z'

  def __sub__(self, t: Self | int) -> Self:
    return self.__class__(self.nanoseconds - (t if isinstance(t, int) else t.nanoseconds))

  def __add__(self, t: Self | int) -> Self:
    return self.__class__(self.nanoseconds + (t if isinstance(t, int) else t.nanoseconds))

  def __eq__(self, t: int) -> bool:
    return self.nanoseconds == t

  @classmethod
  def min(cls) -> Self:
    return cls(cls._NANOSECONDS_MIN)

  @classmethod
  def max(cls) -> Self:
    return cls(cls._NANOSECONDS_MAX)
