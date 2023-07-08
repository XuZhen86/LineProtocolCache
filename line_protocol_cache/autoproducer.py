import sqlite3
from typing import Self
import time
from queue import Queue, Empty
from line_protocol_cache import defaults, sql
from threading import Thread, Event
from absl import logging

_LINE_PROTOCOL_QUEUE: Queue[str] = Queue()


class AutoProducer:

  def __init__(
      self,
      cache_path: str = defaults.CACHE_PATH,
      timeout: float = defaults.TIMEOUT,
  ) -> None:
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
    line_protocols: list[str] = []
    connection = sqlite3.connect(database=self.cache_path, timeout=self.timeout)
    logging.debug('Sqlite3 connection opened.')

    try:
      with connection:
        connection.execute(sql.CREATE_TABLE)
        connection.execute(sql.ENABLE_WAL)
      logging.debug('Sqlite3 connection init finished.')

      while not self._stop_event.is_set():
        try:
          line_protocol = _LINE_PROTOCOL_QUEUE.get_nowait()
        except Empty:
          if len(line_protocols) != 0:
            with connection:
              connection.executemany(sql.INSERT_ROW, [[lp] for lp in line_protocols])
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
        connection.executemany(sql.INSERT_ROW, [[lp] for lp in line_protocols])
      logging.debug(f'Committed {len(line_protocols)} line protocols. (Final)')
    finally:
      connection.close()
      logging.debug('Sqlite3 connection closed.')


def put_line_protocol(line_protocol: str) -> None:
  _LINE_PROTOCOL_QUEUE.put(line_protocol)


def put_line_protocols(line_protocols: list[str]) -> None:
  for lp in line_protocols:
    _LINE_PROTOCOL_QUEUE.put(lp)
