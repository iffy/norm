from zope.interface import implements
from twisted.internet import defer

from collections import deque, defaultdict

from norm.interface import IAsyncCursor, IRunner, IPool



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



class ConnectionPool(object):


    implements(IRunner)

    def __init__(self):
        self._conns = set()


    def add(self, conn):
        self._conns.add(conn)


    def runInteraction(self, function, *args, **kwargs):
        return list(self._conns)[0].runInteraction(function, *args, **kwargs)




class NextAvailablePool(object):
    """
    I give you the next available object in the pool.
    """


    implements(IPool)


    def __init__(self):
        self._options = deque()
        self._pending = deque()
        self._pending_removal = defaultdict(lambda:[])


    def add(self, option):
        self._options.append(option)
        self._fulfillNextPending()


    def remove(self, option):
        try:
            self._options.remove(option)
            return defer.succeed(option)
        except ValueError:
            d = defer.Deferred()
            self._pending_removal[option].append(d)
            return d


    def get(self):
        d = defer.Deferred()
        self._pending.append(d)
        self._fulfillNextPending()
        return d


    def _fulfillNextPending(self):
        if self._pending and self._options:
            self._pending.popleft().callback(self._options.popleft())


    def done(self, option):
        if option in self._pending_removal:
            dlist = self._pending_removal.pop(option)
            map(lambda d: d.callback(option), dlist)
            return
        self._options.append(option)
        self._fulfillNextPending()





