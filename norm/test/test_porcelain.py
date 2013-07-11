# Copyright (c) Matt Haggard.
# See LICENSE for details.

from twisted.trial.unittest import TestCase
from twisted.internet import defer

from norm.porcelain import makePool, insert
from norm.test.util import postgres_url, skip_postgres



class PostgresTest(TestCase):


    timeout = 2
    skip = skip_postgres


    @defer.inlineCallbacks
    def test_basic(self):
        pool = yield makePool(postgres_url)
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

