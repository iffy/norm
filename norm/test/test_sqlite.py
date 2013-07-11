# Copyright (c) Matt Haggard.
# See LICENSE for details.

from twisted.trial.unittest import TestCase
from twisted.internet import defer
from zope.interface.verify import verifyObject

from mock import MagicMock
from norm.sqlite import SqliteCursorWrapper
from norm.interface import IAsyncCursor



class SqliteCursorWrapperTest(TestCase):


    def test_IAsyncCursor(self):
        verifyObject(IAsyncCursor, SqliteCursorWrapper(None))


    def assertCallThrough(self, name, *args, **kwargs):
        mock = MagicMock()
        setattr(mock, name, MagicMock(return_value=defer.succeed('foo')))

        cursor = SqliteCursorWrapper(mock)
        result = getattr(cursor, name)(*args, **kwargs)
        getattr(mock, name).assert_called_once_with(*args, **kwargs)
        self.assertEqual(self.successResultOf(result), 'foo')


    def test_execute(self):
        self.assertCallThrough('execute', 'foo', 'bar')
        self.assertCallThrough('execute', 'foo')


    def test_fetchone(self):
        self.assertCallThrough('fetchone')


    def test_fetchall(self):
        self.assertCallThrough('fetchall')


    def test_lastRowId(self):
        self.assertCallThrough('lastRowId')


    def test_close(self):
        self.assertCallThrough('close')



