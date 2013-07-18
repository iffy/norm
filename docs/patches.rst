Schema management
=================

Norm provides a way to keep track of database patches.  Here's an example:


.. code-block:: python

    from twisted.internet.task import react
    from norm import makePool
    from norm.patch import Patcher

    patcher = Patcher()
    patcher.add('+foo', 'create table foo (id integer primary key, name text)')


    def display(rows):
        assert tuple(rows[0]) == ('foo', 'hey'), rows[0]
        print rows[0]


    def gotPool(pool):
        d = patcher.upgrade(pool)
        d.addCallback(lambda _: pool.runOperation('insert into foo (name) values (?)', ('foo',)))

        d.addCallback(lambda _: patcher.add('+foo.name2', "alter table foo add column name2 text default 'hey'"))
        d.addCallback(lambda _: patcher.upgrade(pool))
        d.addCallback(lambda _: pool.runQuery('select name, name2 from foo'))
        d.addCallback(display)
        return d


    def main(reactor):
        return makePool('sqlite:').addCallback(gotPool)
        

    react(main, [])