from zope.interface import implements
from twisted.internet import defer

from norm.interface import IAsyncCursor, IRunner



class BlockingCursor(object):
    """
    I wrap a single DB-API2 db cursor in an asynchronous api.
    """

    implements(IAsyncCursor)


    def __init__(self, cursor):
        self.cursor = cursor


    def execute(self, sql, params=()):
        return defer.maybeDeferred(self.cursor.execute, sql, params)


    def fetchone(self):
        return defer.maybeDeferred(self.cursor.fetchone)


    def fetchall(self):
        return defer.maybeDeferred(self.cursor.fetchall)


    def lastRowId(self):
        return defer.succeed(self.cursor.lastrowid)



class BlockingRunner(object):
    """
    I wrap a single DB-API2 db connection in an asynchronous api.
    """

    implements(IRunner)

    cursorFactory = BlockingCursor


    def __init__(self, conn):
        self.conn = conn


    def runQuery(self, qry, params=()):
        return self.runInteraction(self._runQuery, qry, params)
    

    def _runQuery(self, cursor, qry, params):
        d = cursor.execute(qry, params)
        d.addCallback(lambda _: cursor.fetchall())
        return d


    def runOperation(self, qry, params=()):
        return self.runInteraction(self._runOperation, qry, params)


    def _runOperation(self, cursor, qry, params):
        return cursor.execute(qry, params)


    def runInteraction(self, function, *args, **kwargs):
        cursor = self.cursorFactory(self.conn.cursor())
        d = defer.maybeDeferred(function, cursor, *args, **kwargs)
        d.addCallback(self._commit)
        d.addErrback(self._rollback)
        return d


    def _commit(self, result):
        self.conn.commit()
        return result


    def _rollback(self, result):
        self.conn.rollback()
        return result


