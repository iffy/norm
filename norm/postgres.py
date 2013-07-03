


# class PostgresTranslator(Tr):
#     """
#     I translate SQL database operations into functions
#     """

#     paramstyle = '%s'


#     def maybeGetResults(self, cursor, operation):
#         try:
#             return cursor.fetchall()
#         except:
#             return []


#     def getLastRowId(self, cursor, operation):
#         cursor.execute('select lastval()')
#         return cursor.fetchone()[0]

