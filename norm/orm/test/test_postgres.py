# Copyright (c) Matt Haggard.
# See LICENSE for details.

from twisted.trial.unittest import TestCase
from twisted.internet import defer

from norm.patch import Patcher
from norm.porcelain import makePool
from norm.postgres import PostgresOperator
from norm.orm.test.mixin import FunctionalIOperatorTestsMixin
from norm.test.util import skip_postgres, postgres_url



class PostgresFunctionalOperatorTest(FunctionalIOperatorTestsMixin, TestCase):

    skip = skip_postgres

    patcher = Patcher()
    patcher.add('only', [
        '''CREATE TABLE empty (
            id SERIAL PRIMARY KEY,
            name bytea,
            uni text,
            date date,
            dtime timestamp,
            mybool boolean
        )''',
        '''CREATE TABLE with_defaults (
            id SERIAL PRIMARY KEY,
            name bytea default 'hey',
            uni text default 'ho',
            date date default '2001-01-01',
            dtime timestamp default '2001-01-01 12:22:32',
            mybool boolean default true
        )''',
        '''CREATE TABLE parent (
            id SERIAL PRIMARY KEY,
            name TEXT
        )''',
        '''CREATE TABLE child (
            id SERIAL PRIMARY KEY,
            name TEXT,
            parent_id INTEGER
        )''',
        '''CREATE TABLE favorite_book (
            child_id INTEGER,
            book_id INTEGER,
            stars INTEGER,
            PRIMARY KEY (child_id, book_id)
        )''',
        '''CREATE TABLE book (
            id SERIAL PRIMARY KEY,
            name text
        )''',
    ])

    tables = [
        'empty', 'with_defaults', 'parent', 'child', 'favorite_book', 'book',
    ]

    @defer.inlineCallbacks
    def deleteData(self, pool):
        for table in self.tables:
            yield pool.runOperation('DELETE FROM %s' % (table,))
        yield pool.close()


    def getOperator(self):
        return PostgresOperator()


    @defer.inlineCallbacks
    def getPool(self):
        pool = yield makePool(postgres_url)
        self.addCleanup(self.deleteData, pool)
        yield self.patcher.upgrade(pool)
        defer.returnValue(pool)

