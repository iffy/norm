from zope.interface import Interface, Attribute


class IAsyncCursor(Interface):


    def execute(query, params=None):
        """
        Execute the given sql and params.

        Return a C{Deferred} which will fire with the DB-API results.
        """


    def fetchone():
        pass


    def fetchall():
        pass


    def lastRowId():
        """
        Return a C{Deferred} id of the most recently inserted row.
        """



class IRunner(Interface):
    """
    I translate and run operations.
    """


    def runQuery(sql, params=None):
        """
        Run a query in a one-off transaction, returning the deferred result.
        """


    def runOperation(sql, params=()):
        """
        Run a query with no results
        """


    def runInteraction(function, *args, **kwargs):
        """
        Run a function within a database transaction.  The function will be
        passed an L{IAsyncCursor} as the first arg.
        """
