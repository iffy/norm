[![Build Status](https://secure.travis-ci.org/iffy/norm.png?branch=master)](http://travis-ci.org/iffy/norm)

# NORM #

An asynchronous, cross-database library (for use with Twisted, for instance).


## How to use it ##

Create a database, add a record (and get the newly created primary key) then
print out all the rows in the table:

    from twisted.internet.task import react
    from norm.common import BlockingRunner
    import sqlite3


    def insertFoo(cursor, name):
        d = cursor.execute('insert into foo (name) values (?)', (name,))
        d.addCallback(lambda _: cursor.lastRowId())
        return d


    def display(results):
        for id, created, name in results:
            print name, created


    def main(reactor):
        db = sqlite3.connect(':memory:')
        runner = BlockingRunner(db)
        
        d = runner.runOperation('''CREATE TABLE foo (
            id integer primary key,
            created timestamp default current_timestamp,
            name text
        )''')
        
        d.addCallback(lambda _: runner.runInteraction(insertFoo, 'something'))
        d.addCallback(lambda rowid: runner.runQuery('select * from foo where id = ?', (rowid,)))
        d.addCallback(display)
        return d


    react(main, [])
