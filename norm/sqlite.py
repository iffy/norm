# Copyright (c) Matt Haggard.
# See LICENSE for details.

__all__ = ['sqlite']

from zope.interface import implements

from norm.interface import IAsyncCursor, IOperator
from norm.orm.base import (classInfo, objectInfo, Converter, BaseOperator)
from norm.orm.props import String, Date, DateTime
from norm.orm.expr import compiler, Compiler

from datetime import datetime


try:
    from pysqlite2 import dbapi2
    sqlite = dbapi2
except:
    import sqlite3 as sqlite


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


    def close(self):
        return self.cursor.close()



toDB = Converter()

@toDB.when(str)
@toDB.when(String)
def stringToDB(pythonval):
    return buffer(pythonval)


fromDB = Converter()

@fromDB.when(String)
def toString(dbval):
    if type(dbval) is unicode:
        return dbval.encode('utf-8')
    elif type(dbval) is buffer:
        return str(dbval)
    return dbval


@fromDB.when(DateTime)
def toDateTime(dbval):
    if type(dbval) is unicode:
        return datetime.strptime(dbval, '%Y-%m-%d %H:%M:%S')
    return dbval


@fromDB.when(Date)
def toDate(dbval):
    if type(dbval) is unicode:
        return datetime.strptime(dbval, '%Y-%m-%d').date()
    return dbval



sqlite_compiler = Compiler([compiler])




class SqliteOperator(BaseOperator):
    """
    I provide SQLite-specific methods for ORM-based database interactions.
    """

    implements(IOperator)

    compiler = sqlite_compiler
    fromDB = fromDB
    toDB = toDB


    def insert(self, cursor, obj):
        """
        Insert a row into the database.  This function expects to be run in
        an asynchronous interaction.
        """
        info = objectInfo(obj)
        cls_info = classInfo(obj)
        changed = info.changed()

        # insert
        insert = ''
        insert_args = []
        if not changed:
            # no changes
            insert = 'INSERT INTO %s DEFAULT VALUES' % (cls_info.table,)
        else:
            # changes
            columns = []
            for prop in changed:
                columns.append(prop.column_name)
                value = self.toDB.convert(prop.__class__, prop.toDatabase(obj))
                insert_args.append(value)
            value_placeholders = ['?'] * len(columns)
            insert = 'INSERT INTO %s (%s) VALUES (%s)' % (cls_info.table,
                        ','.join(columns), ','.join(value_placeholders))


        # select
        columns = cls_info.columns.keys()
        select = 'SELECT %s FROM %s WHERE rowid=?' % (','.join(columns),
                  cls_info.table)

        d = cursor.execute(insert, tuple(insert_args))
        d.addCallback(lambda _: cursor.lastRowId())
        d.addCallback(lambda rowid: cursor.execute(select, (rowid,)))
        d.addCallback(lambda _: cursor.fetchone())
        d.addCallback(self._updateObject, obj)
        return d



