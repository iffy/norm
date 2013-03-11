from zope.interface import implements

from norm.interface import ITranslator, IRunner



class SyncTranslator(object):
    """
    I translate SQL database operations into blocking functions for an SQLite
    connection.
    """

    implements(ITranslator)


    def translate(self, operation):
        return self.translate_Insert(operation)


    def translate_Insert(self, operation):
        def f(x):
            x.execute('insert into %s default values' % (operation.table,))
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
        return func(cursor)
