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
            d = cursor.execute('insert into foo (name) values (?)', (name,))
            d.addCallback(lambda _: cursor.lastRowId())
            return d
        rowid = yield pool.runInteraction(interaction, 'bob')
        rows = yield pool.runQuery('select id, name from foo where id = ?', (rowid,))
        self.assertEqual(rows, [(rowid, 'bob')])

