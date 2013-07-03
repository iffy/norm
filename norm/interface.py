from zope.interface import Interface, Attribute



class IOperation(Interface):
    """
    An atomic operation
    """

    op_name = Attribute("""A unique name for this operation""")



class ITranslator(Interface):
    """
    I translate operations into functions to be run by an L{IRunner}.
    """


    def syncFunction(operation):
        """
        Return a function that can be called with a DB-API2 cursor object to
        perform C{operation}.
        """


    def asyncFunction(operation):
        """
        Return a function that can be called with an L{IAsyncCursor} object to
        perform C{operation}.
        """


    def translateParams(sql):
        """
        Convert an SQL statement using ? to an SQL statement more appropriate
        for the database being translated to.
        """



class IAsyncCursor(Interface):


    def execute(query, params=None):
        """
        Execute the given sql and params.

        Return a C{Deferred} which will fire with the DB-API results.
        """


    def lastRowId():
        """
        Return a C{Deferred} id of the most recently inserted row.
        """


    def close():
        """
        Close the connection.
        """


class IRunner(Interface):
    """
    I translate and run operations.
    """


    def run(operation):
        """
        Translate and run an operation within a transaction.
        """


    def runQuery(sql, params=None):
        """
        Run a query in a one-off transaction, returning the deferred result.
        """


    def runInteraction(function, *args, **kwargs):
        """
        Run a function within a database transaction.  The function will be
        passed an L{IAsyncCursor}.
        """
