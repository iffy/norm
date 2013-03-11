from twisted.trial.unittest import TestCase

from norm.sqlite import SqliteSyncTranslator
from norm.common import SyncRunner
from norm.test.mixin import TranslateRunnerTestMixin


sqlite = None

try:
    from pysqlite2 import dbapi2
    sqlite = dbapi2
except ImportError:
    import sqlite3
    sqlite = sqlite3



class SqliteSyncTranslatorTest(TranslateRunnerTestMixin, TestCase):


    def getConnection(self):
        db = sqlite.connect(':memory:')
        db.execute('''create table foo (
            id integer primary key,
            name text
        )''')
        return db


    def getRunner(self):
        return SyncRunner(self.getConnection())


    def getTranslator(self):
        return SqliteSyncTranslator()


    def test_translateParams(self):
        """
        Should leave ? alone
        """
        trans = SqliteSyncTranslator()
        self.assertEqual(trans.translateParams('select ?'), 'select ?')

