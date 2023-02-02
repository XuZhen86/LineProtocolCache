import sqlite3
from typing import Self

from absl import logging

from line_protocol_cache import defaults, sql

_NO_CONNECTION_VALUE_ERROR_MESSAGE = 'There is no sqlite connection. Are you using "with LineProtocolCacheConsumer(...) as consumer"?'


class LineProtocolCacheConsumer:

  def __init__(
      self,
      cache_path: str = defaults.CACHE_PATH,
      timeout: float = defaults.TIMEOUT,
  ) -> None:
    self.cache_path = cache_path
    self.timeout = timeout
    self._connection: sqlite3.Connection | None = None

  # https://peps.python.org/pep-0343/#motivation-and-summary
  def __enter__(self) -> Self:
    self._connection = sqlite3.connect(database=self.cache_path, timeout=self.timeout)
    self._connection.execute(sql.CREATE_TABLE)
    self._connection.execute(sql.ENABLE_WAL)
    self._connection.commit()
    return self

  def __exit__(self, exception_type, exception_value, exception_traceback) -> None:
    if self._connection is None:
      raise ValueError(_NO_CONNECTION_VALUE_ERROR_MESSAGE)

    self._connection.close()
    self._connection = None

  def get(self) -> dict[int, str]:
    if self._connection is None:
      raise ValueError(_NO_CONNECTION_VALUE_ERROR_MESSAGE)

    rows = self._connection.execute(sql.SELECT_ROWS).fetchall()
    line_protocols: dict[int, str] = {}

    for row in rows:
      if (isinstance(row, tuple) and len(row) == 2 and isinstance(rowid := row[0], int) and
          isinstance(line_protocol := row[1], str)):
        line_protocols[rowid] = line_protocol
      else:
        logging.error('Invalid row: %s. Check query and cache file.', row)

    return line_protocols

  def delete(self, rowids: list[int]) -> None:
    if self._connection is None:
      raise ValueError(_NO_CONNECTION_VALUE_ERROR_MESSAGE)

    self._connection.executemany(sql.DELETE_ROW, [[rowid] for rowid in rowids])
    self._connection.commit()

  def get_max_rowid(self) -> int | None:
    if self._connection is None:
      raise ValueError(_NO_CONNECTION_VALUE_ERROR_MESSAGE)

    rows = self._connection.execute(sql.SELECT_MAX_ROWID).fetchall()

    if len(rows) == 1 and isinstance(row := rows[0], tuple) and len(row) == 1:
      max_rowid = row[0]
      if max_rowid is None:
        return None
      if isinstance(max_rowid, int):
        return max_rowid

    logging.error('Error when querying max_rowid. Check query and cache file.')
    return None
