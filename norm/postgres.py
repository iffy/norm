from norm.common import SyncTranslator



class PostgresSyncTranslator(SyncTranslator):
    """
    I translate SQL database operations into blocking functions for a
    postgres connection.
    """

    paramstyle = '%s'


    def maybeGetResults(self, cursor, operation):
        try:
            return cursor.fetchall()
        except:
            return []


    def getLastRowId(self, cursor, operation):
        cursor.execute('select lastval()')
        return cursor.fetchone()[0]

