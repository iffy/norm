from norm.common import Translator



class SqliteTranslator(Translator):
    """
    I translate database operations for SQLite connections
    """

    paramstyle = '?'


    def getLastRowId(self, cursor, operation):
        return cursor.lastrowid

