from typing import Self

import aiosqlite

from line_protocol_cache import defaults, sql

_NO_CONNECTION_VALUE_ERROR_MESSAGE = 'There is no sqlite connection. Are you using "async with AsyncLineProtocolCacheProducer(...) as producer"?'


class AsyncLineProtocolCacheProducer:

  def __init__(
      self,
      cache_path: str = defaults.CACHE_PATH,
      timeout: float = defaults.TIMEOUT,
  ) -> None:
    self.cache_path = cache_path
    self.timeout = timeout
    self._connection: aiosqlite.Connection | None = None

  # https://peps.python.org/pep-0492/#asynchronous-context-managers-and-async-with
  async def __aenter__(self) -> Self:
    self._connection = await aiosqlite.connect(database=self.cache_path, timeout=self.timeout)
    await self._connection.execute(sql.CREATE_TABLE)
    await self._connection.commit()
    return self

  async def __aexit__(self, exception_type, exception_value, exception_traceback) -> None:
    if self._connection is None:
      raise ValueError(_NO_CONNECTION_VALUE_ERROR_MESSAGE)

    await self._connection.close()
    self._connection = None

  async def put(self, line_protocols: list[str]) -> None:
    if self._connection is None:
      raise ValueError(_NO_CONNECTION_VALUE_ERROR_MESSAGE)

    await self._connection.executemany(sql.INSERT_ROW, [[lp] for lp in line_protocols])
    await self._connection.commit()
