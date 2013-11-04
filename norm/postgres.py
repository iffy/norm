# Copyright (c) Matt Haggard.
# See LICENSE for details.

from zope.interface import implements

from norm.interface import IAsyncCursor
from norm.orm.base import (classInfo, objectInfo, Converter, BaseOperator)
from norm.orm.props import String, Unicode
from norm.orm.expr import compiler, Compiler


def translateSQL(sql):
    # this is naive
    return sql.replace('?', '%s')



class PostgresCursorWrapper(object):


    implements(IAsyncCursor)


    def __init__(self, cursor):
        self.cursor = cursor


    def execute(self, sql, params=()):
        sql = translateSQL(sql)
        ret = self.cursor.execute(sql, params)
        return ret


    def lastRowId(self):
        d = self.cursor.execute('select lastval()')
        d.addCallback(lambda _: self.cursor.fetchone())
        return d.addCallback(lambda row: row[0])


    def fetchone(self):
        return self.cursor.fetchone()


    def fetchall(self):
        return self.cursor.fetchall()


    def close(self):
        return self.cursor.close()



toDB = Converter()

@toDB.when(str)
@toDB.when(String)
def stringToDB(pythonval):
    if pythonval is None:
        return None
    return buffer(pythonval)


fromDB = Converter()

@fromDB.when(String)
def strToString(dbval):
    if type(dbval) is unicode:
        return dbval.encode('utf-8')
    elif type(dbval) is buffer:
        return str(dbval)
    return dbval

@fromDB.when(Unicode)
def unicodeToString(dbval):
    if type(dbval) is unicode:
        return dbval
    elif type(dbval) is str:
        return dbval.decode('utf-8')
    elif type(dbval) is buffer:
        return str(dbval).decode('utf-8')
    return dbval


postgres_compiler = Compiler([compiler])




class PostgresOperator(BaseOperator):
    """
    I provide PostgreSQL-specific methods for ORM-based database interactions.
    """

    compiler = postgres_compiler
    fromDB = fromDB
    toDB = toDB


    def insert(self, cursor, obj):
        """
        Insert a row into the database.  This function expects to be run in an
        asynchronous interaction.
        """
        info = objectInfo(obj)
        cls_info = classInfo(obj)
        changed = info.changed()

        # insert
        insert = []
        insert_args = []
        if not changed:
            # no changes
            insert = ['INSERT INTO %s DEFAULT VALUES' % (cls_info.table,)]
        else:
            # changes
            columns = []
            for prop in changed:
                columns.append(prop.column_name)
                value = toDB.convert(prop.__class__, prop.toDatabase(obj))
                insert_args.append(value)
            value_placeholders = ['?'] * len(columns)
            insert = ['INSERT INTO %s (%s) VALUES (%s)' % (cls_info.table,
                        ','.join(columns), ','.join(value_placeholders))]


        # returning
        columns = cls_info.columns.keys()
        returning = ['RETURNING %s' % (','.join(columns),)]

        sql = ' '.join(insert + returning)
        args = tuple(insert_args)

        d = cursor.execute(sql, args)
        d.addCallback(lambda _: cursor.fetchone())
        d.addCallback(self._updateObject, obj)
        return d



