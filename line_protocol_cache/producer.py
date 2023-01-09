import sqlite3
from typing import Self

from line_protocol_cache import defaults, sql

_NO_CONNECTION_VALUE_ERROR_MESSAGE = 'There is no sqlite connection. Are you using "with LineProtocolCacheProducer(...) as producer"?'


class LineProtocolCacheProducer:

  def __init__(
      self,
      cache_path: str = defaults.CACHE_PATH,
      timeout: float = defaults.TIMEOUT,
  ) -> None:
    self.cache_path = cache_path
    self.timeout = timeout
    self._connection: sqlite3.Connection | None = None

  # https://peps.python.org/pep-0343/#motivation-and-sudmmary
  def __enter__(self) -> Self:
    self._connection = sqlite3.connect(database=self.cache_path, timeout=self.timeout)
    self._connection.execute(sql.CREATE_TABLE)
    self._connection.commit()
    return self

  def __exit__(self, exception_type, exception_value, exception_traceback) -> None:
    if self._connection is None:
      raise ValueError(_NO_CONNECTION_VALUE_ERROR_MESSAGE)

    self._connection.close()
    self._connection = None

  def put(self, line_protocols: list[str]) -> None:
    if self._connection is None:
      raise ValueError(_NO_CONNECTION_VALUE_ERROR_MESSAGE)

    self._connection.executemany(sql.INSERT_ROW, [[lp] for lp in line_protocols])
    self._connection.commit()
