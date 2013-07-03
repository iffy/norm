
from zope.interface import implements

from norm.interface import IAsyncCursor



def translateSQL(sql):
    # this is naive
    return sql.replace('?', '%s')



class PostgresCursorWrapper(object):


    implements(IAsyncCursor)


    def __init__(self, cursor):
        self.cursor = cursor


    def execute(self, sql, params=()):
        sql = translateSQL(sql)
        return self.cursor.execute(sql, params)


    def lastRowId(self):
        d = self.cursor.execute('select lastval()')
        d.addCallback(lambda _: self.cursor.fetchone())
        return d.addCallback(lambda row: row[0])


    def fetchone(self):
        return self.cursor.fetchone()


    def fetchall(self):
        return self.cursor.fetchall()


