from absl import flags


def value_or_default[T](flag_holder: flags.FlagHolder[T]) -> T:
  if flag_holder.present:
    return flag_holder.value
  return flag_holder.default
