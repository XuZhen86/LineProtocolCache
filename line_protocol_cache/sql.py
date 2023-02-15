CREATE_TABLE = '''
    CREATE TABLE IF NOT EXISTS LineProtocolCache (
      line_protocol TEXT NOT NULL
    );
'''

# https://www.sqlite.org/wal.html
ENABLE_WAL = 'PRAGMA journal_mode=WAL;'

INSERT_ROW = '''
    INSERT INTO LineProtocolCache (line_protocol)
    VALUES (?);
'''

# The optimal batch size is 5000 lines of line protocol.
# https://docs.influxdata.com/influxdb/v2.6/write-data/best-practices/optimize-writes/#batch-writes
SELECT_ROWS = '''
    SELECT rowid, line_protocol
    FROM LineProtocolCache
    LIMIT 5000;
'''

DELETE_ROW = '''
    DELETE FROM LineProtocolCache
    WHERE rowid = ?;
'''

SELECT_MAX_ROWID = '''
    SELECT MAX(rowid)
    FROM LineProtocolCache;
'''
