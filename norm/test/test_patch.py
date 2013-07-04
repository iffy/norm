from twisted.trial.unittest import TestCase
from twisted.internet import defer

from norm.sqlite import SqliteTranslator
from norm.common import BlockingRunner
from norm.patch import Patcher, SQLPatch
from norm.operation import SQL
import sqlite3



class PatcherTest(TestCase):


    def getRunner(self):
        db = sqlite3.connect(':memory:')
        runner = BlockingRunner(db, SqliteTranslator())
        self.addCleanup(db.close)
        return runner


    @defer.inlineCallbacks
    def test_add(self):
        """
        You can add patches to a patcher and apply them to
        a database.
        """
        called = []

        patcher = Patcher('_patch')
        r = patcher.add('something', called.append)
        self.assertEqual(r, 1, "Should be patch number 1")

        runner = self.getRunner()

        r = yield patcher.upgrade(runner)
        self.assertEqual(r, [(1, 'something')])
        self.assertEqual(called, [runner], "Should have called the patch "
                         "function with the runner as the only arg")

        called.pop()
        r = yield patcher.upgrade(runner)
        self.assertEqual(r, [], "No new patches should be applied")
        self.assertEqual(called, [], "Should not have applied the patch again")

        rows = yield runner.run(SQL('select number, name from _patch'))
        self.assertEqual(len(rows), 1, "Only one patch applied")
        self.assertEqual(rows[0], (1, 'something'))


    @defer.inlineCallbacks
    def test_commit(self):
        """
        There should be a commit after applying patches
        """
        patcher = Patcher()
        patcher.add('hello', SQLPatch(
            '''create table foo (
                id integer primary key,
                name text
            )''',
            '''insert into foo (name) values ('hey')''',
        ))

        runner = self.getRunner()
        yield patcher.upgrade(runner)
        runner.conn.rollback()
        rows = yield runner.run(SQL('select name from foo'))
        self.assertEqual(len(rows), 1, "Should have committed")


    def test_deferred(self):
        """
        If the patch returns a Deferred, it should hold up
        the patches that follow
        """
        d = defer.Deferred()
        called = []

        patcher = Patcher()
        patcher.add('foo', lambda x:d)
        patcher.add('bar', called.append)

        runner = self.getRunner()
        r = patcher.upgrade(runner)
        self.assertEqual(called, [], "Should not have "
                         "run the bar patch yet")
        d.callback('done')
        self.assertEqual(called, [runner])


