# Copyright (c) Matt Haggard.
# See LICENSE for details.

__all__ = ['makePool', 'insert']



from twisted.internet import defer
from norm.common import BlockingRunner, BlockingCursor, ConnectionPool
from norm.uri import parseURI, mkConnStr
from norm.orm.expr import Query



def _makeSqlite(parsed):
    from norm.sqlite import sqlite
    connstr = mkConnStr(parsed)
    db = sqlite.connect(connstr)
    db.row_factory = sqlite.Row
    runner = BlockingRunner(db)
    runner.db_scheme = 'sqlite'
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
    pool.db_scheme = 'postgres'
    for i in xrange(connections):
        db = psycopg2.connect(connstr, cursor_factory=DictCursor)
        runner = PostgresRunner(db)
        pool.add(runner)
    return defer.succeed(pool)


def _makeTxPostgres(parsed, connections=1):
    from norm.tx_postgres import DictConnection
    pool = ConnectionPool()
    pool.db_scheme = 'postgres'
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
    """
    Make a pool of connections for interacting with a database.

    :param uri: A database URI.  For example, an in-memory SQLite database URI
        looks like this::

            sqlite:

        And a full PostgreSQL URI looks like this::

            postgres://user:password@host.com:1234/databasename

    :param connections: Number of connections in the pool.  This is ignored for
        SQLite databases.

    :rtype: An Deferred IRunner instance which has methods like ``runQuery``,
        ``runOperation`` and ``runInteraction``.  If any of the connections
        fail, this will errback.
    """
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

    .. code-block:: python

        runner = makePool('sqlite:')
        runner.addCallback(insert, 'INSERT INTO foo (name) values (?)', ('joe',))

    :param runner: An IRunner instance (like that returned by ``makePool``)
    :param qry: An SQL string that causes an INSERT to happen (such as
        ``"INSERT ..."`` or ``"REPLACE INTO ..."``)
    :param params: Parameters to be interpolated into the query.
    """
    return runner.runInteraction(_insert, qry, params)



class ORMHandle(object):
    """
    I am a nicer interface for ORMing
    """


    def __init__(self, pool, operator):
        self.pool = pool
        self.operator = operator


    def insert(self, obj):
        return self.pool.runInteraction(self.operator.insert, obj)


    def update(self, obj):
        return self.pool.runInteraction(self.operator.update, obj)


    def refresh(self, obj):
        return self.pool.runInteraction(self.operator.refresh, obj)


    def delete(self, obj):
        return self.pool.runInteraction(self.operator.delete, obj)


    def query(self, query):
        return self.pool.runInteraction(self.operator.query, query)


    def find(self, *args, **kwargs):
        return self.pool.runInteraction(self.operator.query,
                                        Query(*args, **kwargs))


    def transact(self, func, *args, **kwargs):
        return self.pool.runInteraction(self._transact, func, *args, **kwargs)


    def _transact(self, cursor, func, *args, **kwargs):
        inner_handle = _InTransactionORMHandle(cursor, self.operator)
        return func(inner_handle, *args, **kwargs)



class _InTransactionORMHandle(object):
    """
    I am a nice interface
    """


    def __init__(self, cursor, operator):
        self.operator = operator
        self.cursor = cursor


    def insert(self, obj):
        return self.operator.insert(self.cursor, obj)


    def update(self, obj):
        return self.operator.update(self.cursor, obj)


    def delete(self, obj):
        return self.operator.delete(self.cursor, obj)


    def query(self, query):
        return self.operator.query(self.cursor, query)


    def find(self, *args, **kwargs):
        return self.operator.query(self.cursor, Query(*args, **kwargs))


    def refresh(self, obj):
        return self.operator.refresh(self.cursor, obj)



def ormHandle(pool):
    operator = None
    if pool.db_scheme == 'sqlite':
        from norm.sqlite import SqliteOperator
        operator = SqliteOperator()
    elif pool.db_scheme == 'postgres':
        from norm.postgres import PostgresOperator
        operator = PostgresOperator()
    return ORMHandle(pool, operator)



