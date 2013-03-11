from zope.interface import implements

from norm.interface import ITranslator, IRunner



class SyncTranslator(object):
    """
    I translate SQL database operations into blocking functions for a
    postgres connection.
    """

    implements(ITranslator)


    def translate(self, operation):
        handler = getattr(self, 'translate_'+operation.op_name, None)
        return handler(operation)


    def translateParams(self, sql):
        return sql


    def translate_sql(self, operation):
        def f(x):
            x.execute(self.translateParams(operation.sql), operation.args)
            rows = x.fetchall()
            return rows
        return f


    def translate_insert(self, operation):
        def f(x):
            sqls = ['INSERT INTO %s' % (operation.table,)]
            args = []
            if operation.columns:
                names = []
                values = []
                for k,v in operation.columns:
                    names.append(k)
                    values.append('?')
                    args.append(v)
                sqls.append('(%s) values (%s)' % (
                    ','.join(names),
                    ','.join(values),
                ))
            else:
                sqls.append('DEFAULT VALUES')
            sql = ' '.join(sqls)
            x.execute(self.translateParams(sql), tuple(args))
            return x.lastrowid
        return f



class SyncRunner(object):
    """
    I run synchronous database operations.
    """

    implements(IRunner)


    def __init__(self, conn):
        self.conn = conn


    def run(self, func):
        cursor = self.conn.cursor()
        result = func(cursor)
        return result
