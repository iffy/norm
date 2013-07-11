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
    try:
        return _makeTxPostgres(parsed, connections)
    except ImportError:
        return _makeBlockingPostgres(parsed, connections)


def _makeBlockingPostgres(parsed, connections=1):    
    import psycopg2
    from psycopg2.extras import DictCursor
    connstr = mkConnStr(parsed)
    pool = ConnectionPool()
    for i in xrange(connections):
        db = psycopg2.connect(connstr, cursor_factory=DictCursor)
        runner = PostgresRunner(db)
        pool.add(runner)    
    return defer.succeed(pool)


def _makeTxPostgres(parsed, connections=1):
    from norm.tx_postgres import DictConnection
    pool = ConnectionPool()
    connstr = mkConnStr(parsed)

    dlist = []
    for i in xrange(connections):
        conn = DictConnection()
        d = conn.connect(connstr)
        d.addCallback(lambda _: pool.add(conn))
        dlist.append(d)
    ret = defer.gatherResults(dlist)
    return ret.addCallback(lambda _: pool)



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

