# Copyright (c) Matt Haggard.
# See LICENSE for details.

from zope.interface import implements
from txpostgres import txpostgres
import psycopg2.extras

from norm.interface import IAsyncCursor
from norm.postgres import translateSQL



class TxPostgresCursor(txpostgres.Cursor):


    implements(IAsyncCursor)


    def execute(self, sql, params=()):
        sql = translateSQL(sql)
        return txpostgres.Cursor.execute(self, sql, params)


    def lastRowId(self):
        d = self.execute('select lastval()')
        d.addCallback(lambda _: self.fetchone())
        return d.addCallback(lambda row: row[0])



def dict_connect(*args, **kwargs):
    kwargs['connection_factory'] = psycopg2.extras.DictConnection
    return psycopg2.connect(*args, **kwargs)



class DictConnection(txpostgres.Connection):


    cursorFactory = TxPostgresCursor
    connectionFactory = staticmethod(dict_connect)

