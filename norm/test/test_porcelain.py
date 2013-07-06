# Copyright (c) Matt Haggard.
# See LICENSE for details.

from twisted.trial.unittest import TestCase
from twisted.internet import defer

import os

from norm.porcelain import makePool



postgres_url = os.environ.get('NORM_POSTGRESQL_URI', None)
skip_postgres = ('You must define NORM_POSTGRESQL_URI in order to run this '
                 'postgres test')
if postgres_url:
    skip_postgres = ''



class PostgresTest(TestCase):


    timeout = 2
    skip = skip_postgres


    @defer.inlineCallbacks
    def test_basic(self):
        pool = yield makePool(postgres_url)
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



class SqliteTest(TestCase):


    timeout = 2


    @defer.inlineCallbacks
    def test_basic(self):
        pool = yield makePool('sqlite:')
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


