# Copyright (c) Matt Haggard.
# See LICENSE for details.

from twisted.trial.unittest import TestCase, SkipTest
from twisted.internet import defer
from zope.interface.verify import verifyObject

from mock import MagicMock

from norm.interface import IAsyncCursor
from norm.common import BlockingCursor
from norm.postgres import PostgresCursorWrapper
from norm.uri import parseURI, mkConnStr

import os
psycopg2 = None
conn_args = None

try:
    import psycopg2
except ImportError:
    pass


def getConnStr():
    url = os.environ.get('NORM_POSTGRESQL_URI', None)
    if not url:
        raise SkipTest('You must define NORM_POSTGRESQL_URI in order to do '
                       'testing against a postgres database.  It should be '
                       'in the format user:password@host:port/database')
    return mkConnStr(parseURI(url))



class PostgresCursorWrapperTest(TestCase):


    def test_IAsyncCursor(self):
        verifyObject(IAsyncCursor, PostgresCursorWrapper(None))


    def assertCallThrough(self, name, *args, **kwargs):
        mock = MagicMock()
        setattr(mock, name, MagicMock(return_value=defer.succeed('foo')))

        cursor = PostgresCursorWrapper(mock)
        result = getattr(cursor, name)(*args, **kwargs)
        getattr(mock, name).assert_called_once_with(*args, **kwargs)
        self.assertEqual(self.successResultOf(result), 'foo')


    def test_fetchone(self):
        self.assertCallThrough('fetchone')


    def test_fetchall(self):
        self.assertCallThrough('fetchall')



class PostgresCursorWrapperFunctionalTest(TestCase):


    timeout = 2


    def test_works(self):
        connstr = getConnStr()
        db = psycopg2.connect(connstr)
        c = db.cursor()

        wrapped = PostgresCursorWrapper(BlockingCursor(c))

        d = wrapped.execute('create temporary table foo (id serial primary key, name text)')

        # test ? -> %s
        d.addCallback(lambda _: wrapped.execute('insert into foo (name) values (?)', ('foo',)))


        d.addCallback(lambda _: wrapped.lastRowId())
        def check(rowid):
            self.assertEqual(rowid, 1, "Should return last inserted row id")

        return d.addCallback(check)

