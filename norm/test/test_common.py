from twisted.trial.unittest import TestCase
from zope.interface.verify import verifyObject

from mock import MagicMock, create_autospec
import sqlite3

from norm.interface import IAsyncCursor, IRunner
from norm.common import BlockingCursor, BlockingRunner



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



class BlockingRunnerTest(TestCase):


    timeout = 2


    def test_IRunner(self):
        verifyObject(IRunner, BlockingRunner(None))


    def test_cursorFactory(self):
        self.assertEqual(BlockingRunner.cursorFactory, BlockingCursor)


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
            self.assertEqual(result, [
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



