# Copyright (c) Matt Haggard.
# See LICENSE for details.

from zope.interface import implements

from norm.interface import IAsyncCursor, IOperator
from norm.orm.base import classInfo, objectInfo, Converter
from norm.orm.props import Int, String, Unicode, Bool, Date, DateTime

from datetime import datetime, date

try:
    from pysqlite2 import dbapi2 as sqlite
except ImportError:
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


toDB = Converter()

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



class SqliteOperator(object):
    """
    I provide SQLite-specific methods for ORM-based database interactions.
    """

    implements(IOperator)


    def insert(self, cursor, obj):
        """
        Insert a row into the database.  This function expects to be run in
        an interaction.
        """
        info = objectInfo(obj)
        cls_info = classInfo(obj.__class__)
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
                value = toDB.convert(prop.__class__, prop.toDatabase(obj))
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

        # XXX this belongs in a common place
        def updateObject(obj, data):
            for name, props in classInfo(obj.__class__).columns.items():
                if name not in data.keys():
                    continue
                for prop in props:
                    value = fromDB.convert(prop.__class__, data[name])
                    prop.fromDatabase(obj, value)
            return obj
        d.addCallback(lambda row,obj: updateObject(obj, row), obj)
        return d



