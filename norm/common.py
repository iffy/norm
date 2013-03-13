from zope.interface import implements
from twisted.internet import defer

from norm.interface import ITranslator, IRunner



class Translator(object):
    """
    I translate SQL database operations into sql and cursor interactions.
    """

    implements(ITranslator)

    paramstyle = '?'


    # common to async and sync

    def translateParams(self, sql):
        return sql.replace('?', self.paramstyle)


    # asynchronous stuff

    def asyncFunction(self, operation):
        pass


    # synchronous stuff

    def syncFunction(self, operation):
        handler = getattr(self, 'sync_'+operation.op_name, None)
        return handler(operation)


    def sync_sql(self, operation):
        def f(x):
            x.execute(self.translateParams(operation.sql), operation.args)
            return self.maybeGetResults(x, operation)
        return f


    def maybeGetResults(self, cursor, operation):
        return cursor.fetchall()


    def constructInsert(self, operation):
        """
        Create an insert SQL statement.
        """
        sqls = ['INSERT INTO %s' % (operation.table,)]
        args = []
        if operation.columns:
            names = []
            values = []
            for k,v in operation.columns:
                names.append(k)
                values.append('?')
                args.append(v)
            sqls.append('(%s) VALUES (%s)' % (
                ','.join(names),
                ','.join(values),
            ))
        else:
            sqls.append('DEFAULT VALUES')
        sql = ' '.join(sqls)
        return sql, args


    def sync_insert(self, operation):
        def f(x):
            sql, args = self.constructInsert(operation)
            x.execute(self.translateParams(sql), tuple(args))
            if operation.lastrowid:
                return self.getLastRowId(x, operation)
        return f


    def getLastRowId(self, cursor, operation):
        print 'getLastRowId', cursor, operation
        return cursor.lastrowid



class BlockingRunner(object):
    """
    I provide an asynchronous interface for running database operations,
    but I actually block to get the queries done.
    """

    implements(IRunner)


    def __init__(self, conn, translator):
        self.conn = conn
        self.translator = translator


    def run(self, op):
        """
        @return: A C{Deferred} which will fire with the result of C{op}.
        """
        func = self.translator.syncFunction(op)
        cursor = self.conn.cursor()
        return defer.maybeDeferred(func, cursor)


    def runInteraction(self, func, *args, **kwargs):
        """
        @param func: Will be called with an object that has a L{run} method,
            and C{args} and C{kwargs}.  It should call run and expect
            asynchronous results.

        @return: A C{Deferred} which will fire with the result of C{func}.
        """
        runner = BlockingSingleTransactionRunner(self.conn.cursor(),
                                                 self.translator)
        return func(runner, *args, **kwargs)



class BlockingSingleTransactionRunner(object):
    """
    """

    def __init__(self, cursor, translator):
        self.cursor = cursor
        self.translator = translator


    def run(self, op):
        func = self.translator.syncFunction(op)
        return defer.maybeDeferred(func, self.cursor)





