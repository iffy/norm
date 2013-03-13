from zope.interface import implements
from twisted.internet import defer

from norm.interface import ITranslator, IRunner



class SyncTranslator(object):
    """
    I translate SQL database operations into blocking functions
    """

    implements(ITranslator)

    paramstyle = '?'


    def translate(self, operation):
        handler = getattr(self, 'translate_'+operation.op_name, None)
        return handler(operation)


    def translateParams(self, sql):
        return sql.replace('?', self.paramstyle)


    def translate_sql(self, operation):
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


    def translate_insert(self, operation):
        def f(x):
            sql, args = self.constructInsert(operation)
            x.execute(self.translateParams(sql), tuple(args))
            if operation.lastrowid:
                return self.getLastRowId(x, operation)
        return f


    def getLastRowId(self, cursor, operation):
        return cursor.lastrowid



class SyncRunner(object):
    """
    I run synchronous database operations.
    """

    implements(IRunner)


    def __init__(self, conn, translator):
        self.conn = conn
        self.translator = translator


    def run(self, op):
        translated = self.translator.translate(op)
        cursor = self.conn.cursor()
        result = translated(cursor)
        return result



class AdbapiRunner(object):
    """
    I run synchronous database operation in threads using
    C{twisted.enterprise.adbapi}.
    """

    implements(IRunner)


    def __init__(self, pool, translator):
        self.pool = pool
        self.translator = translator


    def run(self, op):
        translated = self.translator.translate(op)
        return self.pool.runInteraction(translated)



