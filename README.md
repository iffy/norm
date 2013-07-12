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


## ORM ##

Included is an deliberately feature-deficient, lightweight ORM.  It makes doing CRUD operations nicer, and that's about it.

The ORM component is largely based on Storm but intentionally leaves out many features that Storm has.

Also, the network interaction is seperate from the
ORMness, so that you could reuse the ORM-ness in a synchronous environment.

Here's an example:


<!--- test:orm_example -->

    from twisted.internet.task import react
    from twisted.internet import defer
    from norm import makePool, ormHandle
    from norm.orm.props import Int, Unicode
    from norm.orm.expr import Query, Eq
    from norm.patch import Patcher


    class Author(object):
        __sql_table__ = 'author'
        id = Int(primary=True)
        name = Unicode()

        def __init__(self, name):
            self.name = name


    class Book(object):
        __sql_table__ = 'book'
        id = Int(primary=True)
        title = Unicode()
        author_id = Int()

        def __init__(self, title, author_id):
            self.title = title
            self.author_id = author_id


    class BookCharacter(object):
        __sql_table__ = 'book_character'
        book_id = Int(primary=True)
        character_id = Int(primary=True)

        def __init__(self, book_id, character_id):
            self.book_id = book_id
            self.character_id = character_id


    class Character(object):
        __sql_table__ = 'character'
        id = Int(primary=True)
        name = Unicode()

        def __init__(self, name):
            self.name = name


    patcher = Patcher()
    patcher.add('tables', [
        '''create table author (
            id integer primary key,
            name text
        )''',
        '''create table book (
            id integer primary key,
            title text,
            author_id integer 
        )''',
        '''create table book_character (
            book_id integer,
            character_id integer,
            primary key (book_id, character_id)
        )''',
        '''create table character (
            id integer primary key,
            name text
        )'''
    ])


    @defer.inlineCallbacks
    def addCSLewisData(handle):
        lewis = yield handle.insert(Author(u'C. S. Lewis'))

        book_names = [
            u'The Lion, the Witch and the Wardrobe',
            u'Prince Caspian: The Return to Narnia',
            u'The Voyage of the Dawn Treader',
        ]
        books = []
        for name in book_names:
            book = yield handle.insert(Book(name, lewis.id))
            books.append(book)

        characters = {
            u'Peter': {
                'obj': None,
                'books': [0, 1],
            },
            u'Susan': {
                'obj': None,
                'books': [0, 1],
            },
            u'Edmund': {
                'obj': None,
                'books': [0, 1, 2],
            },
            u'Lucy': {
                'obj': None,
                'books': [0, 1, 2],
            },
            u'Eustace': {
                'obj': None,
                'books': [2],
            },
        }
        for name, data in characters.items():
            char = yield handle.insert(Character(name))
            for book_idx in data['books']:
                yield handle.insert(BookCharacter(books[book_idx].id, char.id))
    

    @defer.inlineCallbacks
    def handleReady(handle):
        yield addCSLewisData(handle)
        
        books = yield handle.find(Book)
        assert len(books) == 3, books
        for book in books:
            print book.title

        # build up the query little by little
        query = Query(Author, Eq(Author.name, u'C. S. Lewis'))
        query = query.find(Book, Eq(Author.id, Book.author_id))
        query = query.find(BookCharacter, Eq(Book.id, BookCharacter.book_id))
        query = query.find(Character, Eq(BookCharacter.character_id, Character.id))
        chars = yield handle.query(query)

        names = set([x.name for x in chars])
        print names
        assert len(names) == 5, names
        
        # find only the characters in the Dawn Treader (using previous query)
        cs_lewis_dawn_treader = query.find(Character,
            Eq(Book.title, u'The Voyage of the Dawn Treader'))

        chars = yield handle.query(cs_lewis_dawn_treader)
        names = set([x.name for x in chars])
        print names
        assert len(names) == 3, names
        

    def gotPool(pool):
        d = patcher.upgrade(pool)
        d.addCallback(lambda _: ormHandle(pool))
        d.addCallback(handleReady)
        return d


    def main(reactor):
        return makePool('sqlite:foo').addCallback(gotPool)
        
        
    
    react(main, [])

<!--- end -->