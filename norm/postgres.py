from norm.common import SyncTranslator



class PostgresSyncTranslator(SyncTranslator):
    """
    I translate SQL database operations into blocking functions for a
    postgres connection.
    """

    paramstyle = '%s'


    def getLastRowId(self, cursor, operation):
        return cursor.lastrowid

