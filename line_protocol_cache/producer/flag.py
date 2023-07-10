from absl import flags

SQLITE_PATH = flags.DEFINE_string(
    name='sqlite_path',
    default='data/line-protocol-cache/cache.sqlite',
    required=False,
    help='Relative path to the sqlite3 file.',
)

SQLITE_TIMEOUT = flags.DEFINE_float(
    name='sqlite_timeout_s',
    default=60.0,
    required=False,
    help='Sqlite3 timeout in seconds.',
)
