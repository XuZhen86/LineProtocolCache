CREATE_TABLE = '''
    CREATE TABLE IF NOT EXISTS LineProtocolCache (
      line_protocol TEXT NOT NULL
    );
'''

INSERT_ROW = '''
    INSERT INTO LineProtocolCache (line_protocol)
    VALUES (?);
'''

SELECT_ROWS = '''
    SELECT rowid, line_protocol
    FROM LineProtocolCache
    LIMIT 1000;
'''

DELETE_ROW = '''
    DELETE FROM LineProtocolCache
    WHERE rowid = ?;
'''

SELECT_MAX_ROWID = '''
    SELECT MAX(rowid)
    FROM LineProtocolCache;
'''
