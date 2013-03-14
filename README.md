[![Build Status](https://secure.travis-ci.org/iffy/norm.png?branch=master)](http://travis-ci.org/iffy/norm)

# NORM #

An asynchronous, cross-database library (for use with Twisted, for instance).


## How to use it ##

Create a database, add a record (and get the newly created primary key) then
print out all the rows in the table:

    from twisted.internet.task import react
    from norm.sqlite import SqliteTranslator
    from norm.common import BlockingRunner
    from norm.operation import Insert, SQL
    import sqlite3


    def createTables(runner):
        return runner.run(SQL('''CREATE TABLE foo (
            id integer primary key,
            created timestamp default current_timestamp,
            name text
        )''')).addCallback(lambda _:runner)

    def insertFoo(runner, name):
        return runner.run(Insert('foo', [('name', name)], lastrowid=True))

    def selectFoo(rowid, runner):
        return runner.run(SQL('SELECT * FROM foo WHERE id = ?', (rowid,)))

    def display(results):
        for id, created, name in results:
            print name, created


    def main(reactor):
        db = sqlite3.connect(':memory:')
        runner = BlockingRunner(db, SqliteTranslator())
        d = createTables(runner)
        d.addCallback(insertFoo, 'something')
        d.addCallback(selectFoo, runner)
        d.addCallback(display)
        return d


    react(main, [])


If you wanted to switch to PostgreSQL, you would change `SqliteTranslator` to
`PostgresTranslator` and you'd have to change the create statement to include
a serial.

Todo: Make `norm` know that `id integer primary key` should be
`id serial primary key` in PostgreSQL.
