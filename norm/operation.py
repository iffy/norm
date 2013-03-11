"""
SQL operations
"""


class Insert(object):
    """
    Insert a single row into a table and optionally return the newly created
    primary key.
    """

    def __init__(self, table):
        self.table = table
