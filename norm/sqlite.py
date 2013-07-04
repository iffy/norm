from zope.interface import implements

from norm.interface import IAsyncCursor




class SqliteCursorWrapper(object):
    """
    I wrap an IAsyncCursor but do SQLiteish things with it.
    """

    implements(IAsyncCursor)


    def __init__(self, cursor):
        """
        @param cursor: An L{IAsyncCursor} implementing cursor.
        """
        self.cursor = cursor


    def execute(self, *args, **kwargs):
        return self.cursor.execute(*args, **kwargs)


    def fetchone(self):
        return self.cursor.fetchone()


    def fetchall(self):
        return self.cursor.fetchall()


    def lastRowId(self):
        return self.cursor.lastRowId()
