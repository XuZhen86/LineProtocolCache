import sqlite3
import time
from queue import Empty, Queue
from threading import Event, Thread
from typing import Self
import os
from absl import logging

from line_protocol_cache.producer.flag import SQLITE_PATH, SQLITE_TIMEOUT
from line_protocol_cache.sql import CREATE_TABLE, ENABLE_WAL, INSERT_ROW

_LINE_PROTOCOL_QUEUE: Queue[str] = Queue()


class AutoProducer:

  def __init__(self, cache_path: str | None = None, timeout: float | None = None) -> None:
    if cache_path is None:
      cache_path = str(SQLITE_PATH.value)
    if timeout is None:
      timeout = float(SQLITE_TIMEOUT.value)

    self.cache_path = cache_path
    self.timeout = timeout
    self._thread = Thread(target=self._auto_put, name='AutoProducer')
    self._stop_event = Event()

  def __enter__(self) -> Self:
    self._thread.start()
    logging.info(f'AutoProducer thread@{self._thread.native_id} has started.')
    return self

  def __exit__(self, exception_type, exception_value, exception_traceback) -> None:
    logging.info(f'Stopping AutoProducer thread@{self._thread.native_id}.')
    self._stop_event.set()
    self._thread.join()
    logging.info(f'AutoProducer thread@{self._thread.native_id} has stopped.')

  async def __aenter__(self) -> Self:
    return self.__enter__()

  async def __aexit__(self, exception_type, exception_value, exception_traceback) -> None:
    self.__exit__(exception_type, exception_value, exception_traceback)

  def _auto_put(self) -> None:
    os.makedirs(os.path.dirname(self.cache_path), exist_ok=True)

    connection = sqlite3.connect(database=self.cache_path, timeout=self.timeout)
    logging.debug(f'Sqlite3 connection opened. {self.cache_path=}, {self.timeout=}')

    try:
      with connection:
        connection.execute(CREATE_TABLE)
        connection.execute(ENABLE_WAL)
      logging.debug('Sqlite3 connection init finished.')

      line_protocols: list[str] = []

      while not self._stop_event.is_set():
        try:
          line_protocol = _LINE_PROTOCOL_QUEUE.get_nowait()
        except Empty:
          if len(line_protocols) != 0:
            with connection:
              connection.executemany(INSERT_ROW, [[lp] for lp in line_protocols])
            logging.debug(f'Committed {len(line_protocols)} line protocols.')
            line_protocols.clear()
          time.sleep(1)
        else:
          logging.debug(f'{line_protocol=}')
          line_protocols.append(line_protocol)

      while True:
        try:
          line_protocol = _LINE_PROTOCOL_QUEUE.get_nowait()
        except Empty:
          break
        else:
          logging.debug(f'{line_protocol=}')
          line_protocols.append(line_protocol)

      with connection:
        connection.executemany(INSERT_ROW, [[lp] for lp in line_protocols])
      logging.debug(f'Committed {len(line_protocols)} line protocols. (Final)')
    finally:
      connection.close()
      logging.debug('Sqlite3 connection closed.')


def put_line_protocol(line_protocol: str) -> None:
  _LINE_PROTOCOL_QUEUE.put(line_protocol)


def put_line_protocols(line_protocols: list[str]) -> None:
  for lp in line_protocols:
    _LINE_PROTOCOL_QUEUE.put(lp)
