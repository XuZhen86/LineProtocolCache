import math
import os
import sqlite3
from dataclasses import dataclass
from queue import Queue
from threading import Event, Thread
from typing import Iterable, Self

from absl import logging
from influxdb_client import Point


@dataclass(frozen=True)
class LineProtocolCacheConfig:
  # Relative or absolute path to the SQLite cache file. More files may be created under the same path.
  cache_path: str = 'data/line-protocol-cache/cache.sqlite'

  # SQLite timeout in seconds. Adjust this limit if the file system is lagging.
  timeout_s: float = 20.0

  # Max number of line protocol strings to be insterted into the SQLite cache file at the same time. Reduce this number if database limits are exceeded.
  batch_size: int = 1_000

  # Interval in seconds to commit line protocols to the SQLite cache file.
  commit_interval_s: float = 2.0

  # Time in seconds between selecting a lucky line protocol to be printed. Supply value 0 to print every line protocol.
  sample_interval_s: float = math.inf

  def __post_init__(self) -> None:
    assert self.timeout_s > 0
    assert self.batch_size > 0
    assert self.commit_interval_s > 0
    assert self.sample_interval_s >= 0


class LineProtocolCache:
  _ENABLE_WAL = 'PRAGMA journal_mode=WAL;'  # https://www.sqlite.org/wal.html
  _CREATE_TABLE = 'CREATE TABLE IF NOT EXISTS LineProtocolCache (line_protocol TEXT NOT NULL);'
  _INSERT_ROW = 'INSERT INTO LineProtocolCache (line_protocol) VALUES (?);'

  _QUEUE: Queue[str] = Queue()
  _IS_QUEUE_OPEN = Event()

  def __init__(self, config: LineProtocolCacheConfig = LineProtocolCacheConfig()) -> None:
    self._config = config
    self._thread = Thread(target=self._drain_queue, name='LineProtocolCache')
    self._stop_thread = Event()

  def __enter__(self) -> Self:
    logging.info('Starting LineProtocolCache thread.')
    self._thread.start()
    self._IS_QUEUE_OPEN.wait()
    logging.info(f'Thread @{self._thread.native_id} has started.')
    return self

  def __exit__(self, exception_type, exception_value, exception_traceback) -> None:
    logging.info(f'Stopping thread @{self._thread.native_id}.')
    self._stop_thread.set()
    self._thread.join()
    logging.info(f'Thread @{self._thread.native_id} has stopped.')

  async def __aenter__(self) -> Self:
    return self.__enter__()

  async def __aexit__(self, exception_type, exception_value, exception_traceback) -> None:
    self.__exit__(exception_type, exception_value, exception_traceback)

  def _drain_queue(self) -> None:
    os.makedirs(os.path.dirname(self._config.cache_path), exist_ok=True)
    connection = sqlite3.connect(database=self._config.cache_path, timeout=self._config.timeout_s)

    try:
      with connection:
        connection.execute(self._ENABLE_WAL)
        connection.execute(self._CREATE_TABLE)
      self._IS_QUEUE_OPEN.set()

      # Sleep, but wake up earlier if the thread needs to be stopped.
      while not self._stop_thread.wait(self._config.commit_interval_s):
        self._insert_rows(connection, self._get_rows())

      # Get all of the remaining rows before exiting.
      while self._QUEUE.qsize() != 0:
        self._insert_rows(connection, self._get_rows())
    finally:
      # Keep the queue open until the final moment to collect any last bit of strings.
      # It also raises an exception in put() in case the thread is dead.
      self._IS_QUEUE_OPEN.clear()
      connection.close()

  def _get_rows(self) -> list[str]:
    rows: list[str] = []

    while self._QUEUE.qsize() != 0 and len(rows) < self._config.batch_size:
      row = self._QUEUE.get()
      rows.append(row)
      logging.log_every_n_seconds(logging.INFO, row, self._config.sample_interval_s)

    return rows

  def _insert_rows(self, connection: sqlite3.Connection, rows: list[str]) -> None:
    if len(rows) == 0:
      return

    with connection:
      connection.executemany(self._INSERT_ROW, [(row,) for row in rows])
    for _ in range(len(rows)):
      self._QUEUE.task_done()

  @classmethod
  def put(cls, *items: Point | Iterable[Point]) -> None:
    if not cls._IS_QUEUE_OPEN.is_set():
      raise ValueError('Line protocol queue is not open. '
                       'Check if there was an Exception in the LineProtocolCache thread.')

    for item in items:
      if isinstance(item, Point):
        cls._QUEUE.put(item.to_line_protocol())
      else:
        for point in item:
          cls._QUEUE.put(point.to_line_protocol())
