from typing import TypeVar

from absl import flags

T = TypeVar(name='T')


def value_or_default(flag_holder: flags.FlagHolder[T]) -> T:
  if flag_holder.present:
    return flag_holder.value
  return flag_holder.default
