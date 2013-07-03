# Copyright (c) Matt Haggard.
# See LICENSE for details.

from twisted.internet import defer
from norm.common import BlockingRunner, BlockingCursor, ConnectionPool
from norm.uri import parseURI, mkConnStr



def _makeSqlite(parsed):
    try:
        from pysqlite2 import dbapi2 as sqlite
    except ImportError:
        import sqlite3 as sqlite
    connstr = mkConnStr(parsed)
    db = sqlite.connect(connstr)
    runner = BlockingRunner(db)
    return defer.succeed(runner)



class PostgresRunner(BlockingRunner):

    def cursorFactory(self, cursor):
        from norm.postgres import PostgresCursorWrapper
        return PostgresCursorWrapper(BlockingCursor(cursor))



def _makePostgres(parsed, connections=1):
    import psycopg2
    connstr = mkConnStr(parsed)
    pool = ConnectionPool()
    for i in xrange(connections):
        db = psycopg2.connect(connstr)
        runner = PostgresRunner(db)
        pool.add(runner)    
    return defer.succeed(pool)



def makePool(uri, connections=1):
    parsed = parseURI(uri)
    if parsed['scheme'] == 'sqlite':
        return _makeSqlite(parsed)
    elif parsed['scheme'] == 'postgres':
        return _makePostgres(parsed, connections)
    else:
        raise Exception('%s is not supported' % (parsed['scheme'],))