# Copyright (c) Matt Haggard.
# See LICENSE for details.

__all__ = ['makePool', 'insert']



from twisted.internet import defer
from norm.common import BlockingRunner, BlockingCursor, ConnectionPool
from norm.uri import parseURI, mkConnStr



def _makeSqlite(parsed):
    from norm.sqlite import sqlite
    connstr = mkConnStr(parsed)
    db = sqlite.connect(connstr)
    db.row_factory = sqlite.Row
    runner = BlockingRunner(db)
    return defer.succeed(runner)



class PostgresRunner(BlockingRunner):


    def cursorFactory(self, cursor):
        from norm.postgres import PostgresCursorWrapper
        return PostgresCursorWrapper(BlockingCursor(cursor))



def _makePostgres(parsed, connections=1):
    import psycopg2
    from psycopg2.extras import DictCursor
    connstr = mkConnStr(parsed)
    pool = ConnectionPool()
    for i in xrange(connections):
        db = psycopg2.connect(connstr, cursor_factory=DictCursor)
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



def _insert(cursor, qry, params):
    d = cursor.execute(qry, params)
    return d.addCallback(lambda _: cursor.lastRowId())


def insert(runner, qry, params=()):
    """
    Run an INSERT-like query and return the id of the newly created row.
    """
    return runner.runInteraction(_insert, qry, params)

