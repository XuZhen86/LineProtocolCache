import signal
from threading import Event
from typing import List

from absl import app

from .migrationactuator import MigrationActuator


def main(args: List[str]) -> None:
  stop_running = Event()
  signal.signal(signal.SIGTERM, lambda signal_number, stack_frame: stop_running.set())

  MigrationActuator().run(stop_running)


def app_run_main() -> None:
  app.run(main)
