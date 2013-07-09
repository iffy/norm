# Copyright (c) Matt Haggard.
# See LICENSE for details.

from norm.orm.base import classInfo



class Query(object):
    """
    XXX
    """


    def __init__(self, select):
        if type(select) not in (list, tuple):
            select = (select,)
        self.select = select
        self._tables = []
        self._props = []
        self._process()


    def _process(self):
        self._tables = []
        self._props = []
        for item in self.select:
            info = classInfo(item)
            keys = sorted(info.attributes.values())
            self._props.extend(keys)
            self._tables.append(info.table)


    def sql(self):
        """
        Return the SQL and args that will do this query.
        """
        columns = [x.column_name for x in self.properties()]
        sql = 'SELECT %s FROM %s' % (','.join(columns), self._tables[0])
        return sql, ()


    def properties(self):
        """
        Get a tuple of the Properties that will be returned by the query.
        """
        return tuple(self._props)


    def tables(self):
        return self._tables