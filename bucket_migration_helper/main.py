import signal
from threading import Event

from absl import app

from .migrationactuator import MigrationActuator


def main(args: list[str]) -> None:
  stop_running = Event()
  signal.signal(signal.SIGTERM, lambda signal_number, stack_frame: stop_running.set())

  MigrationActuator().run(stop_running)


def app_run_main() -> None:
  app.run(main)
