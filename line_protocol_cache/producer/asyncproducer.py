import os
from typing import Self

import aiosqlite
from absl import logging

from line_protocol_cache.producer.flag import SQLITE_PATH, SQLITE_TIMEOUT
from line_protocol_cache.sql import CREATE_TABLE, ENABLE_WAL, INSERT_ROW

_NO_CONNECTION_VALUE_ERROR_MESSAGE = 'There is no sqlite connection. Are you using "async with AsyncProducer(...) as producer"?'


class AsyncProducer:

  def __init__(self, cache_path: str | None = None, timeout: float | None = None) -> None:
    if cache_path is None:
      cache_path = str(SQLITE_PATH.value)
    if timeout is None:
      timeout = float(SQLITE_TIMEOUT.value)

    self.cache_path = cache_path
    self.timeout = timeout
    self._connection: aiosqlite.Connection | None = None

  # https://peps.python.org/pep-0492/#asynchronous-context-managers-and-async-with
  async def __aenter__(self) -> Self:
    os.makedirs(os.path.dirname(self.cache_path), exist_ok=True)

    self._connection = await aiosqlite.connect(database=self.cache_path, timeout=self.timeout)
    logging.debug(f'Sqlite3 connection opened. {self.cache_path=}, {self.timeout=}')

    async with self._connection:
      await self._connection.execute(CREATE_TABLE)
      await self._connection.execute(ENABLE_WAL)
    logging.debug('Sqlite3 connection init finished.')

    return self

  async def __aexit__(self, exception_type, exception_value, exception_traceback) -> None:
    if self._connection is None:
      raise ValueError(_NO_CONNECTION_VALUE_ERROR_MESSAGE)

    await self._connection.close()
    self._connection = None
    logging.debug('Sqlite3 connection closed.')

  async def put(self, line_protocols: list[str]) -> None:
    if self._connection is None:
      raise ValueError(_NO_CONNECTION_VALUE_ERROR_MESSAGE)

    async with self._connection:
      await self._connection.executemany(INSERT_ROW, [[lp] for lp in line_protocols])
    logging.debug(f'Committed {len(line_protocols)} line protocols.')
