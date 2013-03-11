"""
SQL operations
"""

from zope.interface import implements

from norm.interface import IOperation



class Insert(object):
    """
    Insert a single row into a table and optionally return the newly created
    primary key.
    """

    implements(IOperation)
    op_name = 'insert'


    def __init__(self, table, columns=None, id_column=None):
        self.table = table
        self.columns = columns
        self.id_column = id_column



class SQL(object):
    """
    Execute some SQL
    """

    implements(IOperation)
    op_name = 'sql'


    def __init__(self, sql, args=None):
        self.sql = sql
        self.args = args or ()
