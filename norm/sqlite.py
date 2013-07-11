# Copyright (c) Matt Haggard.
# See LICENSE for details.

__all__ = ['sqlite']

from zope.interface import implements

from norm.interface import IAsyncCursor, IOperator
from norm.orm.base import classInfo, objectInfo, Converter, reconstitute
from norm.orm.props import String, Date, DateTime
from norm.orm.expr import compiler, State, Compiler, Query, Join, Table
from norm.orm.error import NotFound

from datetime import datetime


try:
    from pysqlite2 import dbapi2 as sqlite
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

@sqlite_compiler.when(Query)
def compile_Query(query, state):
    # select
    props = query.properties()
    columns = []
    select_args = []
    for prop in props:
        s, q = state.compile(prop)
        columns.append(s)
        select_args.extend(q)
    select_clause = ['SELECT %s' % (','.join(columns),)]

    # where
    where_clause = []
    where_args = []
    constraints = query.constraints
    if constraints:
        s, a = state.compile(constraints)
        where_clause = ['WHERE %s' % (s,)]
        where_args.extend(a)

    # table
    classes = [x for x in state.classes]
    from_args = []
    tables = []
    for cls in classes:
        s, a = state.compile(Table(cls))
        tables.append(s)
        from_args.extend(a)

    from_clause = ['FROM %s' % (','.join(tables))]
    

    sql = ' '.join(select_clause + from_clause + where_clause)
    args = tuple(select_args + from_args + where_args)
    return sql, args


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
        d.addCallback(self._updateObject, obj)
        return d


    def _makeObjects(self, rows, query):
        ret = []
        props = query.properties()
        for row in rows:
            data = zip(props, row)
            data = [(x[0], fromDB.convert(x[0].__class__, x[1])) for x in data]
            ret.append(reconstitute(data))
        return ret


    def _updateObject(self, data, obj):
        if data is None:
            raise NotFound(obj)
        for name, props in classInfo(obj.__class__).columns.items():
            if name not in data.keys():
                continue
            for prop in props:
                value = fromDB.convert(prop.__class__, data[name])
                prop.fromDatabase(obj, value)
        return obj


    def query(self, cursor, query):
        """
        XXX
        """
        sql, args = sqlite_compiler.compile(query)
        d = cursor.execute(sql, tuple(args))
        d.addCallback(self._makeObjects, query)
        return d


    def refresh(self, cursor, obj):
        """
        XXX
        """
        info = classInfo(obj.__class__)
        
        args = []
        
        where_parts = []
        for prop in info.primaries:
            where_parts.append('%s=?' % (prop.column_name,))
            args.append(toDB.convert(prop.__class__, prop.toDatabase(obj)))
        
        columns = info.columns.keys()
        select = 'SELECT %s FROM %s WHERE %s' % (','.join(columns),
                  info.table, ' AND '.join(where_parts))

        d = cursor.execute(select, tuple(args))
        d.addCallback(lambda _: cursor.fetchone())
        d.addCallback(self._updateObject, obj)
        return d


    def update(self, cursor, obj):
        """
        XXX
        """
        obj_info = objectInfo(obj)
        info = classInfo(obj.__class__)
        changed = obj_info.changed()

        # XXX I copied and modified this from insert
        set_parts = []
        set_args = []
        for prop in changed:
            set_parts.append('%s=?' % (prop.column_name,))
            value = toDB.convert(prop.__class__, prop.toDatabase(obj))
            set_args.append(value)

        # XXX I copied this from refresh
        # XXX REFACTOR
        where_parts = []
        where_args = []
        for prop in info.primaries:
            where_parts.append('%s=?' % (prop.column_name,))
            where_args.append(toDB.convert(prop.__class__, prop.toDatabase(obj)))

        update = 'UPDATE %s SET %s WHERE %s' % (info.table,
                 ','.join(set_parts),
                 ' AND '.join(where_parts))
        args = tuple(set_args + where_args)

        return cursor.execute(update, args)


    def delete(self, cursor, obj):
        """
        XXX
        """
        info = classInfo(obj.__class__)

        # XXX I copied this from refresh
        # XXX REFACTOR
        where_parts = []
        where_args = []
        for prop in info.primaries:
            where_parts.append('%s=?' % (prop.column_name,))
            where_args.append(toDB.convert(prop.__class__, prop.toDatabase(obj)))

        delete = 'DELETE FROM %s WHERE %s' % (info.table,
                 ' AND '.join(where_parts))

        args = tuple(where_args)

        return cursor.execute(delete, args)



