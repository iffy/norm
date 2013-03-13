from twisted.trial.unittest import TestCase

from norm.sqlite import SqliteTranslator
from norm.common import BlockingRunner
from norm.test.mixin import TranslateRunnerTestMixin


sqlite = None
sqlite_module = None

try:
    from pysqlite2 import dbapi2
    sqlite = dbapi2
    sqlite_module = 'pysqlite2.dbapi2'
except ImportError:
    import sqlite3
    sqlite = sqlite3
    sqlite_module = 'sqlite3'



class SqliteBlockingTest(TranslateRunnerTestMixin, TestCase):


    def getConnection(self):
        db = sqlite.connect(':memory:')
        db.execute('''create table foo (
            id integer primary key,
            name text
        )''')
        return db


    def getRunner(self, translator):
        return BlockingRunner(self.getConnection(), translator)


    def getTranslator(self):
        return SqliteTranslator()


    def test_translateParams(self):
        """
        Should leave ? alone
        """
        trans = SqliteTranslator()
        self.assertEqual(trans.translateParams('select ?'), 'select ?')
