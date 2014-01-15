# Copyright (c) Matt Haggard.
# See LICENSE for details.

from twisted.trial.unittest import TestCase
from twisted.internet import defer
from zope.interface.verify import verifyObject

from mock import MagicMock, create_autospec
import sqlite3

from norm.interface import IAsyncCursor, IRunner, IPool
from norm.common import (BlockingCursor, BlockingRunner, ConnectionPool,
                         NextAvailablePool)



class BlockingCursorTest(TestCase):


    timeout = 2


    def test_IAsyncCursor(self):
        verifyObject(IAsyncCursor, BlockingCursor(None))


    def test_execute(self):
        """
        You can execute queries in pretended asynchronousness
        """
        db = sqlite3.connect(':memory:')
        cursor = BlockingCursor(db.cursor())
        d = cursor.execute('create table foo (name text)')
        d.addCallback(lambda _: cursor.execute('insert into foo (name) values(?)', ('name1',)))
        d.addCallback(lambda _: cursor.execute('select name from foo'))
        d.addCallback(lambda _: cursor.fetchone())
        def check(result):
            self.assertEqual(result, ('name1',))
        d.addCallback(check)
        return d


    def test_fetchall(self):
        """
        You can fetch all
        """
        db = sqlite3.connect(':memory:')
        cursor = BlockingCursor(db.cursor())
        d = cursor.execute('create table foo (name text)')
        d.addCallback(lambda _: cursor.execute('insert into foo (name) values(?)', ('name1',)))
        d.addCallback(lambda _: cursor.execute('select name from foo'))
        d.addCallback(lambda _: cursor.fetchall())
        def check(result):
            self.assertEqual(result, [('name1',)])
        d.addCallback(check)
        return d


    def test_lastrowid(self):
        """
        You can get the lastrowid (which may be meaningless for some db cursors)
        """
        mock = MagicMock()
        mock.lastrowid = 12
        cursor = BlockingCursor(mock)
        d = cursor.lastRowId()
        d.addCallback(lambda rowid: self.assertEqual(rowid, 12))
        return d


    def test_close(self):
        """
        You can close the cursor
        """
        mock = MagicMock()
        mock.close = MagicMock()
        cursor = BlockingCursor(mock)
        d = cursor.close()
        d.addCallback(lambda _: mock.close.assert_called_once_with())
        return d



class BlockingRunnerTest(TestCase):


    timeout = 2


    def test_IRunner(self):
        verifyObject(IRunner, BlockingRunner(sqlite3.connect(':memory:')))


    def test_cursorFactory(self):
        self.assertEqual(BlockingRunner.cursorFactory, BlockingCursor)


    def test_close(self):
        """
        You can close the runner
        """
        mock = MagicMock()
        runner = BlockingRunner(mock)

        d = runner.close()
        d.addCallback(lambda _: mock.close.assert_called_once_with())
        return d


    def test_runInteraction(self):
        """
        Should call the function with an instance of cursorFactory
        """
        db = sqlite3.connect(':memory:')
        mock = create_autospec(db)
        runner = BlockingRunner(mock)

        def interaction(cursor, *args, **kwargs):
            self.assertTrue(isinstance(cursor, BlockingCursor))
            self.assertEqual(args, (1,2,3))
            self.assertEqual(kwargs, {'foo': 'bar'})
            return 'result'

        def check(result):
            self.assertEqual(result, 'result')
            mock.commit.assert_called_once_with()

        d = runner.runInteraction(interaction, 1, 2, 3, foo='bar')
        return d.addCallback(check)


    def test_runInteraction_error(self):
        """
        If there's an error in the interaction, do a rollback
        """
        db = sqlite3.connect(':memory:')
        mock = create_autospec(db)
        runner = BlockingRunner(mock)

        def interaction(cursor):
            raise Exception('foo')


        def check(result):
            mock.rollback.assert_called_once_with()


        d = runner.runInteraction(interaction)
        return d.addErrback(check)


    def test_runQuery(self):
        """
        Should run an interaction that runs the query and returns results.
        """
        db = sqlite3.connect(':memory:')
        db.execute('create table foo (name text)')
        db.execute('insert into foo (name) values (?)', ('name1',))
        db.execute('insert into foo (name) values (?)', ('name2',))

        runner = BlockingRunner(db)

        d = runner.runQuery('select name from foo order by name')
        def check(result):
            self.assertEqual(map(tuple,result), [
                ('name1',),
                ('name2',),
            ])
        return d.addCallback(check)


    def test_runOperation(self):
        """
        Should run an interaction that runs the query but doesn't return
        results.
        """
        db = sqlite3.connect(':memory:')

        runner = BlockingRunner(db)

        d = runner.runOperation('create table foo (name text)')
        def done(_):
            db.execute('insert into foo (name) values (?)', ('name1',))
        return d.addCallback(done)



class ConnectionPoolTest(TestCase):

    timeout = 2

    def test_IRunner(self):
        verifyObject(IRunner, ConnectionPool())


    def test_setConnect(self):
        """
        You can tell the connection pool how to make connections.
        """
        called = []
        def mkConnection(arg):
            called.append(arg)
            return 'connection'
        pool = ConnectionPool()
        pool.setConnect(mkConnection, 'foo')
        d = pool.makeConnection()
        self.assertEqual(self.successResultOf(d), 'connection')
        self.assertEqual(called, ['foo'])


    def test_scheme(self):
        """
        Should be aware of its database scheme
        """
        pool = ConnectionPool()
        self.assertEqual(pool.db_scheme, None)


    def test_add(self):
        """
        You can add connections to a pool
        """
        mock = MagicMock()

        balancer = MagicMock()

        pool = ConnectionPool(pool=balancer)
        pool.add(mock)
        balancer.add.assert_called_once_with(mock)


    def test_runInteraction(self):
        """
        You can run an interaction
        """
        mock = MagicMock()
        mock.runInteraction = MagicMock(return_value=defer.succeed('success'))

        pool = ConnectionPool()
        pool.add(mock)

        d = pool.runInteraction('my interaction')
        self.assertEqual(self.successResultOf(d), 'success')
        mock.runInteraction.assert_called_once_with('my interaction')


    def test_runQuery(self):
        """
        You can run a query
        """
        mock = MagicMock()
        mock.runQuery = MagicMock(return_value=defer.succeed('success'))

        pool = ConnectionPool()
        pool.add(mock)

        d = pool.runQuery('my query')
        self.assertEqual(self.successResultOf(d), 'success')
        mock.runQuery.assert_called_once_with('my query')


    def test_runOperation(self):
        """
        You can run a query
        """
        mock = MagicMock()
        mock.runOperation = MagicMock(return_value=defer.succeed('success'))

        pool = ConnectionPool()
        pool.add(mock)

        d = pool.runOperation('my query')
        self.assertEqual(self.successResultOf(d), 'success')
        mock.runOperation.assert_called_once_with('my query')


    def test_returnToPool(self):
        """
        After a successful query, interaction or operation, the connection
        should be returned to the pool
        """
        mock = MagicMock()
        mock.runInteraction = MagicMock(return_value=defer.fail(Exception('foo')))
        mock.runQuery = MagicMock(return_value=defer.succeed('something'))
        mock.runOperation = MagicMock(return_value=defer.succeed('success'))

        pool = ConnectionPool()
        pool.add(mock)

        d = pool.runQuery('query')
        self.assertEqual(self.successResultOf(d), 'something')
        d = pool.runOperation('operation')
        self.assertEqual(self.successResultOf(d), 'success')
        d = pool.runInteraction('interaction')
        self.assertIsInstance(self.failureResultOf(d).value, Exception)


    def test_close(self):
        """
        You can close all the connections
        """
        c1 = MagicMock()
        c2 = MagicMock()

        pool = ConnectionPool()
        pool.add(c1)
        pool.add(c2)

        d = pool.close()
        d.addCallback(lambda _: c1.close.assert_called_once_with())
        d.addCallback(lambda _: c2.close.assert_called_once_with())
        return d


    @defer.inlineCallbacks
    def test_reconnect(self):
        """
        If a query fails, make sure it wasn't just closed, reconnect and retry
        if it was.
        """
        class BadConn(object):

            def __init__(self):
                self.called = []

            def runQuery(self, *args):
                self.called.append(args)
                return defer.fail(Exception('foo'))

        c1 = BadConn()

        c2 = MagicMock()
        c2.runQuery.return_value = defer.succeed('success')

        pool = ConnectionPool()
        pool.add(c1)
        pool.setConnect(lambda:c2)

        result = yield pool.runQuery('something', 'ran')
        self.assertEqual(len(c1.called), 2)
        self.assertIn(('something', 'ran'), c1.called)
        c2.runQuery.assert_any_call('something', 'ran')
        self.assertEqual(pool.pool.list(), [c2], "Should have the new conn "
                         "in the pool")
        self.assertEqual(result, 'success', "Should have eventually succeeded")


    def test_reconnect_noConnectionMethod(self):
        """
        Reconnection is not possible is the setConnect method hasn't been
        called.
        """
        exc = Exception('foo')

        class BadConn(object):

            def __init__(self):
                self.called = []

            def runQuery(self, *args):
                self.called.append(args)
                return defer.fail(exc)

        c1 = BadConn()
        pool = ConnectionPool()
        pool.add(c1)

        result = pool.runQuery('something', 'ran')

        self.assertEqual(self.failureResultOf(result).value, exc)
        self.assertEqual(len(c1.called), 1)
        self.assertIn(('something', 'ran'), c1.called)


class NextAvailablePoolTest(TestCase):


    timeout = 2


    def test_IPool(self):
        verifyObject(IPool, NextAvailablePool())


    @defer.inlineCallbacks
    def test_common(self):
        pool = NextAvailablePool()

        pool.add('foo')
        pool.add('bar')

        a = yield pool.get()
        yield pool.get()
        c = pool.get()
        self.assertFalse(c.called, "Shouldn't have any available")

        yield pool.done(a)
        self.assertTrue(c.called, "The pending request should get the "
                        "newly available thing")


    def test_add_pending(self):
        """
        If a new option is added, it should fulfill pending requests
        """
        pool = NextAvailablePool()

        d = pool.get()
        self.assertFalse(d.called)

        pool.add('foo')
        self.assertTrue(d.called, "Should fulfill pending request")


    @defer.inlineCallbacks
    def test_remove(self):
        """
        If the option isn't being used, removal should happen immediately
        """
        pool = NextAvailablePool()

        pool.add('foo')
        pool.add('bar')
        r = yield pool.remove('foo')
        self.assertEqual(r, 'foo')

        a = yield pool.get()
        self.assertEqual(a, 'bar')
        b = pool.get()
        self.assertEqual(b.called, False)
        pool.done(a)
        self.assertEqual(b.called, True)


    @defer.inlineCallbacks
    def test_remove_pending(self):
        """
        If the option is in use, don't remove it until its done being used.
        """
        pool = NextAvailablePool()
        pool.add('foo')
        a = yield pool.get()

        b = pool.remove('foo')
        self.assertFalse(b.called, "Don't remove it yet because it's being used")
        pool.done(a)
        self.assertEqual(self.successResultOf(b), 'foo')


    @defer.inlineCallbacks
    def test_remove_twice(self):
        """
        If you request removal twice, both removals will be fulfilled
        """
        pool = NextAvailablePool()
        pool.add('foo')
        a = yield pool.get()

        b = pool.remove('foo')
        c = pool.remove('foo')
        pool.done(a)
        self.assertEqual(self.successResultOf(b), 'foo')
        self.assertEqual(self.successResultOf(c), 'foo')


    @defer.inlineCallbacks
    def test_list(self):
        """
        You can list all the things in the pool
        """
        pool = NextAvailablePool()
        pool.add('foo')
        pool.add('bar')
        yield pool.get()
        pool.add('choo')
        pool.add('bozo')
        yield pool.remove('bozo')

        r = pool.list()
        self.assertEqual(set(r), set(['foo', 'bar', 'choo']))












