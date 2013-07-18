# Copyright (c) Matt Haggard.
# See LICENSE for details.

from twisted.trial.unittest import TestCase
from twisted.internet import defer

from norm.porcelain import makePool, insert, ormHandle
from norm.patch import Patcher
from norm.test.util import postgres_url, skip_postgres
from norm.orm.props import Int
from norm.orm.expr import Eq, Query



class PostgresTest(TestCase):


    timeout = 2
    skip = skip_postgres


    @defer.inlineCallbacks
    def test_basic(self):
        pool = yield makePool(postgres_url)
        self.assertEqual(pool.db_scheme, 'postgres')
        self.addCleanup(pool.close)
        yield pool.runOperation('''CREATE TEMPORARY TABLE porc1 (
            id serial primary key,
            created timestamp default current_timestamp,
            name text
        )''')

        def interaction(cursor, name):
            d = cursor.execute('insert into porc1 (name) values (?)', (name,))
            d.addCallback(lambda _: cursor.lastRowId())
            return d
        rowid = yield pool.runInteraction(interaction, 'bob')
        rows = yield pool.runQuery('select id, name from porc1 where id = ?', (rowid,))
        self.assertEqual(map(tuple,rows), [(rowid, 'bob')])
        self.assertEqual(rows[0]['id'], rowid)
        self.assertEqual(rows[0]['name'], 'bob')


    @defer.inlineCallbacks
    def test_insert(self):
        pool = yield makePool(postgres_url)
        self.addCleanup(pool.close)
        yield pool.runOperation('''CREATE TEMPORARY TABLE porc2 (
            id serial primary key,
            name text
        )''')


        rowid = yield insert(pool, 'insert into porc2 (name) values (?)', ('bob',))
        self.assertEqual(rowid, 1)



class SqliteTest(TestCase):


    timeout = 2


    @defer.inlineCallbacks
    def test_basic(self):
        pool = yield makePool('sqlite:')
        self.assertEqual(pool.db_scheme, 'sqlite')
        self.addCleanup(pool.close)
        yield pool.runOperation('''CREATE TABLE porc1 (
            id integer primary key,
            created timestamp default current_timestamp,
            name text
        )''')

        def interaction(cursor, name):
            d = cursor.execute('insert into porc1 (name) values (?)', (name,))
            d.addCallback(lambda _: cursor.lastRowId())
            return d
        rowid = yield pool.runInteraction(interaction, 'bob')
        rows = yield pool.runQuery('select id, name from porc1 where id = ?', (rowid,))
        self.assertEqual(map(tuple,rows), [(rowid, 'bob')])
        self.assertEqual(rows[0]['id'], rowid)
        self.assertEqual(rows[0]['name'], 'bob')


    @defer.inlineCallbacks
    def test_insert(self):
        pool = yield makePool('sqlite:')
        self.addCleanup(pool.close)
        yield pool.runOperation('''CREATE TABLE porc2 (
            id integer primary key,
            name text
        )''')

        rowid = yield insert(pool, 'insert into porc2 (name) values (?)', ('bob',))
        self.assertEqual(rowid, 1)



class ormHandleMixin(object):


    class Foo(object):
        __sql_table__ = 'porc3'
        id = Int(primary=True)
        age = Int()


    def getPool(self):
        raise NotImplementedError("Implement getPool")


    @defer.inlineCallbacks
    def test_pool(self):
        """
        The ormHandle should have a pool attribute that you can use directly.
        """
        pool = yield self.getPool()
        handle = yield ormHandle(pool)
        self.assertEqual(handle.pool, pool)


    @defer.inlineCallbacks
    def test_functional(self):
        pool = yield self.getPool()

        handle = yield ormHandle(pool)

        foo = yield handle.insert(self.Foo())
        foo.age = 22

        yield handle.update(foo)

        foos = yield handle.find(self.Foo)
        foo2 = foos[0]
        self.assertEqual(foo2.age, 22)
        foo2.age = 23

        foos = yield handle.query(Query(self.Foo))
        self.assertEqual(len(foos), 1)

        yield handle.update(foo2)
        yield handle.refresh(foo)
        self.assertEqual(foo.age, 23)

        foos = yield handle.find(self.Foo, Eq(self.Foo.age, 20))
        self.assertEqual(len(foos), 0)

        yield handle.delete(foo)

        foos = yield handle.find(self.Foo)
        self.assertEqual(len(foos), 0)


    @defer.inlineCallbacks
    def test_interaction(self):
        """
        You can do a bunch of things in a transaction
        """
        pool = yield self.getPool()

        handle = yield ormHandle(pool)

        @defer.inlineCallbacks
        def interaction(handle):
            foo = yield handle.insert(self.Foo())
            foo.age = 22

            yield handle.update(foo)

            foos = yield handle.find(self.Foo)
            foo2 = foos[0]
            self.assertEqual(foo2.age, 22)
            foo2.age = 23

            foos = yield handle.query(Query(self.Foo))
            self.assertEqual(len(foos), 1)

            yield handle.update(foo2)
            yield handle.refresh(foo)
            self.assertEqual(foo.age, 23)

            foos = yield handle.find(self.Foo, Eq(self.Foo.age, 20))
            self.assertEqual(len(foos), 0)

            yield handle.delete(foo)
            foos = yield handle.find(self.Foo)
            self.assertEqual(len(foos), 0)

        yield handle.transact(interaction)


    @defer.inlineCallbacks
    def test_interaction_commitOnSuccess(self):
        """
        The transaction should be committed on success.
        """
        pool = yield self.getPool()

        handle = yield ormHandle(pool)
        foo = yield handle.insert(self.Foo())

        @defer.inlineCallbacks
        def interaction(handle, foo):
            yield handle.delete(foo)

        yield handle.transact(interaction, foo)

        foos = yield handle.find(self.Foo)
        self.assertEqual(len(foos), 0, "Should have deleted the object")


    @defer.inlineCallbacks
    def test_interaction_rollbackOnError(self):
        """
        The transaction will be rolled back on error
        """
        pool = yield self.getPool()

        handle = yield ormHandle(pool)
        foo = yield handle.insert(self.Foo())

        @defer.inlineCallbacks
        def interaction(handle, foo):
            yield handle.delete(foo)
            raise Exception('error')

        d = handle.transact(interaction, foo)
        self.assertFailure(d, Exception)

        foos = yield handle.find(self.Foo)
        self.assertEqual(len(foos), 1, "Should not have deleted the object")



class SqliteOrmHandleTest(ormHandleMixin, TestCase):


    timeout = 2

    patcher = Patcher()
    patcher.add('foo', [
        '''CREATE TABLE porc3 (
            id INTEGER PRIMARY KEY,
            age INTEGER
        )''',
    ])

    @defer.inlineCallbacks
    def getPool(self):
        pool = yield makePool('sqlite:')
        self.addCleanup(pool.close)
        yield self.patcher.upgrade(pool)
        defer.returnValue(pool)



class PostgresOrmHandleTest(ormHandleMixin, TestCase):


    timeout = 2
    skip = skip_postgres

    patcher = Patcher()
    patcher.add('foo', [
        '''CREATE TABLE porc3 (
            id SERIAL PRIMARY KEY,
            age INTEGER
        )''',
    ])

    def cleanTable(self, pool):
        d = pool.runOperation('delete from porc3')
        return d.addCallback(lambda _: pool.close())


    @defer.inlineCallbacks
    def getPool(self):
        pool = yield makePool(postgres_url)
        self.addCleanup(self.cleanTable, pool)
        yield self.patcher.upgrade(pool)
        defer.returnValue(pool)

