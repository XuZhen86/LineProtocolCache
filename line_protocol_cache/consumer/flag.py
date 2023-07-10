from typing import Any

from absl import flags
import line_protocol_cache.producer.flag as producer_flag

SQLITE_PATH = producer_flag.SQLITE_PATH

SQLITE_TIMEOUT = producer_flag.SQLITE_TIMEOUT

LOG_EVERY_N_SECONDS = flags.DEFINE_integer(
    name='log_every_n_seconds',
    default=None,
    required=False,
    help='Slow the logging down to logging every N seconds. '
    'Directly passed into absl.logging.log_every_n_seconds(). '
    'Use value 0 to log every string.',
)

SERVER_URL = flags.DEFINE_string(
    name='server_url',
    default=None,
    required=True,
    help='InfluxDB server API url (ex. http://localhost:8086).',
)
BUCKET = flags.DEFINE_string(
    name='bucket',
    default=None,
    required=True,
    help='specifies the destination bucket for collected metrics.',
)
BUCKET_TOKEN = flags.DEFINE_string(
    name='bucket_token',
    default=None,
    required=True,
    help='token to authenticate to the InfluxDB 2.x.',
)
BUCKET_ORG = flags.DEFINE_string(
    name='bucket_org',
    default=None,
    required=True,
    help='organization name.',
)

DERIVED_SERVER_URL = flags.DEFINE_string(
    name='derived_server_url',
    default=None,
    help='InfluxDB server API url (ex. http://localhost:8086).',
)
DERIVED_BUCKET = flags.DEFINE_string(
    name='derived_bucket',
    default=None,
    help='specifies the destination bucket for collected metrics.',
)
DERIVED_BUCKET_TOKEN = flags.DEFINE_string(
    name='derived_bucket_token',
    default=None,
    help='token to authenticate to the InfluxDB 2.x.',
)
DERIVED_BUCKET_ORG = flags.DEFINE_string(
    name='derived_bucket_org',
    default=None,
    help='organization name.',
)

HTTP_TIMEOUT_MILLISECONDS = flags.DEFINE_integer(
    name='http_timeout_milliseconds',
    default=10_000,
    help='HTTP client timeout setting for a request specified in milliseconds.',
)


def _flag_presense_validator(flag: dict[str, Any]) -> bool:
  print(flag.values())

  is_present = list(flag.values())[0] is not None

  for name, value in flag.items():
    if (value is not None) != is_present:
      raise flags.ValidationError(
          f'Flag {name} expected to {"" if is_present else "not"} be present.')

  return True


flags.register_multi_flags_validator(
    [DERIVED_SERVER_URL, DERIVED_BUCKET, DERIVED_BUCKET_TOKEN, DERIVED_BUCKET_ORG],
    _flag_presense_validator,
)
