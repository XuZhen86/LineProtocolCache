import sqlite3
from typing import Self

from absl import logging
import os

from line_protocol_cache.consumer.flag import LOG_EVERY_N_SECONDS, SQLITE_PATH, SQLITE_TIMEOUT
from line_protocol_cache.sql import (CREATE_TABLE, DELETE_ROW, ENABLE_WAL, SELECT_MAX_ROWID,
                                     SELECT_ROWS)

_NO_CONNECTION_VALUE_ERROR_MESSAGE = 'There is no sqlite connection. Are you using "with Consumer(...) as consumer"?'


class Consumer:

  def __init__(self, cache_path: str | None = None, timeout: float | None = None) -> None:
    if cache_path is None:
      cache_path = str(SQLITE_PATH.value)
    if timeout is None:
      timeout = float(SQLITE_TIMEOUT.value)

    self.cache_path = cache_path
    self.timeout = timeout
    self._connection: sqlite3.Connection | None = None

  # https://peps.python.org/pep-0343/#motivation-and-summary
  def __enter__(self) -> Self:
    os.makedirs(os.path.dirname(self.cache_path), exist_ok=True)

    self._connection = sqlite3.connect(database=self.cache_path, timeout=self.timeout)
    logging.info(f'Sqlite3 connection opened. {self.cache_path=}, {self.timeout=}')

    with self._connection:
      self._connection.execute(CREATE_TABLE)
      self._connection.execute(ENABLE_WAL)
    logging.info('Sqlite3 connection init finished.')

    return self

  def __exit__(self, exception_type, exception_value, exception_traceback) -> None:
    if self._connection is None:
      raise ValueError(_NO_CONNECTION_VALUE_ERROR_MESSAGE)

    self._connection.close()
    self._connection = None
    logging.info('Sqlite3 connection closed.')

  def get(self) -> dict[int, str]:
    if self._connection is None:
      raise ValueError(_NO_CONNECTION_VALUE_ERROR_MESSAGE)

    with self._connection:
      rows = self._connection.execute(SELECT_ROWS).fetchall()
    line_protocols: dict[int, str] = {}

    for row in rows:
      if (not isinstance(row, tuple) or len(row) != 2 or not isinstance(rowid := row[0], int)
          or not isinstance(line_protocol := row[1], str)):
        e = ValueError('Invalid row. Check query and cache file.')
        e.add_note(f'{row=}')
        raise e

      line_protocols[rowid] = line_protocol
      if LOG_EVERY_N_SECONDS.present:
        logging.log_every_n_seconds(logging.INFO, f'{rowid=}, {line_protocol=}',
                                    int(LOG_EVERY_N_SECONDS.value))

    logging.info(f'Read {len(line_protocols)} line protocols.')
    return line_protocols

  def delete(self, rowids: list[int]) -> None:
    if self._connection is None:
      raise ValueError(_NO_CONNECTION_VALUE_ERROR_MESSAGE)

    with self._connection:
      self._connection.executemany(DELETE_ROW, [[rowid] for rowid in rowids])
    logging.info(f'Deleted {len(rowids)} line protocols.')

  def max_rowid(self) -> int | None:
    if self._connection is None:
      raise ValueError(_NO_CONNECTION_VALUE_ERROR_MESSAGE)

    with self._connection:
      rows = self._connection.execute(SELECT_MAX_ROWID).fetchall()

    if len(rows) != 1 or not isinstance(row := rows[0], tuple) or len(row) != 1:
      e = ValueError('Error when querying max_rowid. Check query and cache file.')
      e.add_note(f'{rows=}')
      raise e

    max_rowid = row[0]
    if max_rowid is None:
      return None
    if isinstance(max_rowid, int):
      return max_rowid

    e = ValueError('Got unexpected type when querying max_rowid.')
    e.add_note(f'{max_rowid=}')
    e.add_note(f'{type(max_rowid)=}')
    raise e
