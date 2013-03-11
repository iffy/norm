from norm.common import SyncTranslator



class SqliteSyncTranslator(SyncTranslator):
    """
    I translate SQL database operations into blocking functions for an SQLite
    connection.
    """

    paramstyle = '?'


    def getLastRowId(self, cursor, operation):
        return cursor.lastrowid

