# Copyright (c) Matt Haggard.
# See LICENSE for details.

from twisted.trial.unittest import TestCase
from twisted.internet import defer
from zope.interface.verify import verifyObject

from mock import MagicMock

from norm.interface import IAsyncCursor
from norm.common import BlockingCursor
from norm.postgres import PostgresCursorWrapper
from norm.test.util import postgresConnStr



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


    def test_close(self):
        self.assertCallThrough('close')



class PostgresCursorWrapperFunctionalTest(TestCase):


    timeout = 2


    def test_works(self):
        connstr = postgresConnStr()
        import psycopg2
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


