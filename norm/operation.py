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


    def __init__(self, table, columns=None, lastrowid=False):
        """
        @param table: table name to insert into.
        @param columns: A list of 2-tuples of the form (column_name,value).
        @param lastrowid: If C{True}, then the last insert row id should be
            returned when I am run.
        """
        self.table = table
        self.columns = columns
        self.lastrowid = lastrowid



class SQL(object):
    """
    Execute some SQL
    """

    implements(IOperation)
    op_name = 'sql'


    def __init__(self, sql, args=None):
        self.sql = sql
        self.args = args or ()
