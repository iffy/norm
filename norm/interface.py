# Copyright (c) Matt Haggard.
# See LICENSE for details.

from zope.interface import Interface



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


    def close():
        pass



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


    def close():
        """
        Close the connections
        """



class IPool(Interface):


    def add(option):
        """
        Add a ready option to the pool
        """


    def remove(option):
        """
        Remove an option from the pool
        """


    def get():
        """
        Choose the next option; this will fire with a Deferred when the next
        thing is ready for use.
        """


    def done(option):
        """
        Accept an option as ready
        """


    def list():
        """
        List all the things
        """



class IOperator(Interface):
    """
    I provide functions for doing operations.  My methods are suitable for
    use as the function in L{IRunner.runInteraction}.
    """


    def insert(cursor, obj):
        """
        Insert the ORM object into the database, update the object's attributes
        and return the object.
        """


    def query(cursor, query):
        """
        XXX
        """


    def refresh(cursor, obj):
        """
        XXX
        """


    def update(cursor, obj):
        """
        XXX
        """


    def delete(cursor, obj):
        """
        XXX
        """





