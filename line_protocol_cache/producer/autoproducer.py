import os
import sqlite3
from queue import Queue
from threading import Event, Thread
from typing import Self

from absl import logging
from influxdb_client import Point

from line_protocol_cache.producer.flag import SQLITE_PATH, SQLITE_TIMEOUT
from line_protocol_cache.sql import CREATE_TABLE, ENABLE_WAL, INSERT_ROW


class AutoProducer:
  QUEUE: Queue[str] = Queue()
  IS_QUEUE_OPEN = Event()

  def __init__(self, cache_path: str | None = None, timeout: float | None = None) -> None:
    if cache_path is None:
      cache_path = str(SQLITE_PATH.value)
    if timeout is None:
      timeout = float(SQLITE_TIMEOUT.value)

    self.cache_path = cache_path
    self.timeout = timeout
    self._thread = Thread(target=self._drain_queue, name='AutoProducer')
    self._stop_thread = Event()

  def __enter__(self) -> Self:
    self._thread.start()
    self.IS_QUEUE_OPEN.wait()
    logging.info(f'AutoProducer thread@{self._thread.native_id} has started.')
    return self

  def __exit__(self, exception_type, exception_value, exception_traceback) -> None:
    logging.info(f'Stopping AutoProducer thread@{self._thread.native_id}.')
    self._stop_thread.set()
    self._thread.join()
    logging.info(f'AutoProducer thread@{self._thread.native_id} has stopped.')

  async def __aenter__(self) -> Self:
    return self.__enter__()

  async def __aexit__(self, exception_type, exception_value, exception_traceback) -> None:
    self.__exit__(exception_type, exception_value, exception_traceback)

  def _drain_queue(self) -> None:
    os.makedirs(os.path.dirname(self.cache_path), exist_ok=True)
    connection = sqlite3.connect(database=self.cache_path, timeout=self.timeout)
    logging.debug(f'Sqlite3 connection opened. {self.cache_path=}, {self.timeout=}')

    try:
      with connection:
        connection.execute(CREATE_TABLE)
        connection.execute(ENABLE_WAL)
      self.IS_QUEUE_OPEN.set()
      logging.debug('Sqlite3 connection init finished.')

      while not self._stop_thread.is_set():
        # Sleep for 1 second, but wake up earlier if the thread needs to be stopped.
        self._stop_thread.wait(1)
        # Intentionally draining the queue after sleeping, so that all rows up until the event are committed.
        self._insert_rows(connection, self._get_rows())
    finally:
      self.IS_QUEUE_OPEN.clear()
      connection.close()
      logging.debug('Sqlite3 connection closed.')

  def _get_rows(self) -> list[str]:
    rows: list[str] = []

    while self.QUEUE.qsize() != 0:
      row = self.QUEUE.get()
      logging.debug(f'{row}')
      rows.append(row)

    return rows

  def _insert_rows(self, connection: sqlite3.Connection, rows: list[str]) -> None:
    if len(rows) == 0:
      return

    with connection:
      connection.executemany(INSERT_ROW, [[row] for row in rows])
    for _ in range(len(rows)):
      self.QUEUE.task_done()
    logging.debug(f'Committed {len(rows)} line protocols.')


def put_line_protocol(line_protocol: str) -> None:
  if not AutoProducer.IS_QUEUE_OPEN.isSet():
    raise ValueError('Line protocol queue is not open.')
  AutoProducer.QUEUE.put(line_protocol)


def put_line_protocols(line_protocols: list[str]) -> None:
  if not AutoProducer.IS_QUEUE_OPEN.isSet():
    raise ValueError('Line protocol queue is not open.')
  for lp in line_protocols:
    AutoProducer.QUEUE.put(lp)


def put_point(point: Point) -> None:
  if not AutoProducer.IS_QUEUE_OPEN.isSet():
    raise ValueError('Line protocol queue is not open.')
  AutoProducer.QUEUE.put(point.to_line_protocol())


def put_points(points: list[Point]) -> None:
  if not AutoProducer.IS_QUEUE_OPEN.isSet():
    raise ValueError('Line protocol queue is not open.')
  for p in points:
    AutoProducer.QUEUE.put(p.to_line_protocol())
