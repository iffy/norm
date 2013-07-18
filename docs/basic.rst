Basic usage
===========

You can use norm to connect to and interact with databases asynchronously.
And you can use question marks for all database parameters:


.. code-block:: python

    from twisted.internet.task import react
    from norm import makePool


    def insertFoo(cursor, name):
        d = cursor.execute('insert into foo (name) values (?)', (name,))
        d.addCallback(lambda _: cursor.lastRowId())
        return d


    def display(results):
        for id, created, name in results:
            print name, created


    def gotPool(pool):
        d = pool.runOperation('''CREATE TABLE foo (
            id integer primary key,
            created timestamp default current_timestamp,
            name text
        )''')

        d.addCallback(lambda _: pool.runInteraction(insertFoo, 'something'))
        d.addCallback(lambda rowid: pool.runQuery('select * from foo where id = ?', (rowid,)))
        d.addCallback(display)
        return d

    def main(reactor):
        return makePool('sqlite:').addCallback(gotPool)
        

    react(main, [])


.. autofunction:: norm.makePool

.. autofunction:: norm.insert