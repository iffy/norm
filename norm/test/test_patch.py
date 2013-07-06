# Copyright (c) Matt Haggard.
# See LICENSE for details.

from twisted.trial.unittest import TestCase
from twisted.internet import defer

from norm import makePool
from norm.patch import Patcher, SQLPatch



class PatcherTest(TestCase):


    def getPool(self):
        return makePool('sqlite:')


    @defer.inlineCallbacks
    def test_add(self):
        """
        You can add patches to a patcher and apply them to
        a database.
        """
        called = []

        patcher = Patcher('_patch')
        patcher.add('something', called.append)

        pool = yield self.getPool()

        r = yield patcher.upgrade(pool)
        self.assertEqual(r, ['something'])
        self.assertEqual(len(called), 1,
                         "Should have called the patch function")

        called.pop()
        r = yield patcher.upgrade(pool)
        self.assertEqual(r, [], "No new patches should be applied")
        self.assertEqual(called, [], "Should not have applied the patch again")

        rows = yield pool.runQuery('select name from _patch')
        self.assertEqual(len(rows), 1, "Only one patch applied")
        self.assertEqual(rows[0]['name'], 'something')


    @defer.inlineCallbacks
    def test_add_SQLPatch_default(self):
        """
        You can use a single string or a list/tuple of strings,
        instead of using SQLPatch directly.
        """
        patcher = Patcher('_patch')
        patcher.add('something', [
            'create table bar (name text)',
            'create table bar2 (name text)',
        ])
        patcher.add('another',
            'create table bar3 (name text)')

        pool = yield self.getPool()
        applied = yield patcher.upgrade(pool)
        self.assertEqual(applied, ['something', 'another'], "Should return the"
                         " names of the patches applied")

        yield pool.runOperation('insert into bar (name) values (?)',
                                ('hey',))
        yield pool.runOperation('insert into bar2 (name) values (?)',
                                ('hey',))
        yield pool.runOperation('insert into bar3 (name) values (?)',
                                ('hey',))


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

        pool = yield self.getPool()
        yield patcher.upgrade(pool)
        pool.conn.rollback()
        rows = yield pool.runQuery('select name from foo')
        self.assertEqual(len(rows), 1, "Should have committed")


    @defer.inlineCallbacks
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

        pool = yield self.getPool()
        yield patcher.upgrade(pool)
        self.assertEqual(called, [], "Should not have "
                         "run the bar patch yet")
        d.callback('done')
        self.assertEqual(len(called), 1)


    def test_uniquePatchNames(self):
        """
        Patch names must be unique
        """
        patcher = Patcher()
        patcher.add('foo', 'foo')
        self.assertRaises(Exception, patcher.add, 'foo', 'bar')


    @defer.inlineCallbacks
    def test_partialUpgrade(self):
        """
        You can stop at a particular patch.
        """
        pool = yield self.getPool()

        patcher = Patcher()
        patcher.add('foo', 'create table foo (name text)')
        patcher.add('bar', "insert into foo (name) values ('hey')")

        patches = yield patcher.upgrade(pool, 'foo')
        self.assertEqual(patches, ['foo'], "Only foo should have been applied")

        count = yield pool.runQuery('select count(*) from foo')
        self.assertEqual(count[0][0], 0, "Should not have run the bar patch")

        yield patcher.upgrade(pool)

        count = yield pool.runQuery('select count(*) from foo')
        self.assertEqual(count[0][0], 1, "Should have run the bar patch")







