# Copyright (c) Matt Haggard.
# See LICENSE for details.

from twisted.trial.unittest import TestCase
from twisted.internet import defer


from norm.patch import Patcher
from norm.porcelain import makePool
from norm.sqlite import SqliteOperator
from norm.orm.test.mixin import FunctionalIOperatorTestsMixin



class SqliteFunctionalOperatorTest(FunctionalIOperatorTestsMixin, TestCase):

    patcher = Patcher()
    patcher.add('only', [
        '''CREATE TABLE empty (
            id INTEGER PRIMARY KEY,
            name blob,
            uni text,
            date date,
            dtime timestamp,
            mybool tinyint
        )''',
        '''CREATE TABLE with_defaults (
            id INTEGER PRIMARY KEY,
            name blob default 'hey',
            uni text default 'ho',
            date date default '2001-01-01',
            dtime timestamp default '2001-01-01 12:22:32',
            mybool tinyint default 1
        )''',
        '''CREATE TABLE parent (
            id INTEGER PRIMARY KEY,
            name TEXT
        )''',
        '''CREATE TABLE child (
            id INTEGER PRIMARY KEY,
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
            id INTEGER PRIMARY KEY,
            name text
        )''',
    ])


    def getOperator(self):
        return SqliteOperator()


    @defer.inlineCallbacks
    def getPool(self):
        pool = yield makePool('sqlite:')
        yield self.patcher.upgrade(pool)
        defer.returnValue(pool)

