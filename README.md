[![Build Status](https://secure.travis-ci.org/iffy/norm.png?branch=master)](http://travis-ci.org/iffy/norm)

# NORM #

An asynchronous, cross-database library (for use with Twisted, for instance).


## Basic usage ##

Create a database, add a record (and get the newly created primary key) then
print out all the rows in the table:

<!--- test:example1 -->

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

<!--- end -->


## Schema migrations / patches ##

Keep track of schema changes:


<!--- test:example2 -->

    from twisted.internet.task import react
    from norm import makePool
    from norm.patch import Patcher

    patcher = Patcher()
    patcher.add('+foo', 'create table foo (id integer primary key, name text)')


    def display(rows):
        assert rows[0] == ('foo', 'hey'), rows[0]
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

<!--- end -->


You will typically have a single `Patcher` instance per database type in a file
to which you add patches as needed, like this:

<!--- test:example3 -->
    
    from norm.patch import Patcher

    patcher = Patcher()
    patcher.add('+customer', [
        '''CREATE TABLE customer (
            id INTEGER PRIMARY KEY,
            created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            email TEXT,
            name TEXT
        )''',
        'CREATE UNIQUE INDEX customer_email_idx ON customer(email)',
    ])

    patcher.add('+invitation',
        '''CREATE TABLE invitation (
            id INTEGER PRIMARY KEY,
            created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            email TEXT,
            accepted TIMESTAMP
        )''',
    )

    patcher.add('+customer.lastlogin',
        'ALTER TABLE customer ADD COLUMN lastlogin TIMESTAMP')


<!--- end -->

Patches are lists of SQL by default, but you may also provide a python function
to do more complicated patching techniques.

### Partial migration ###

You can choose to only apply up to a certain patch.  This is useful for testing
migrations.


<!--- test:partial_migration -->

    from twisted.internet import defer, task
    from norm import makePool
    from norm.patch import Patcher

    patcher = Patcher()
    patcher.add('+foo', 'create table foo (name text)')
    patcher.add('+add default user', "insert into foo (name) values ('admin')")

    @defer.inlineCallbacks
    def gotPool(pool):
        yield patcher.upgrade(pool, '+foo')
        rows = yield pool.runQuery('select count(*) from foo')

        print rows[0][0]
        assert rows[0][0] == 0, rows

        yield patcher.upgrade(pool)
        rows = yield pool.runQuery('select count(*) from foo')

        print rows[0][0]
        assert rows[0][0] == 1, rows




    def main(reactor):
        return makePool('sqlite:').addCallback(gotPool)


    task.react(main, [])

<!--- end -->