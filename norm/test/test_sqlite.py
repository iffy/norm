from twisted.trial.unittest import TestCase
from twisted.enterprise import adbapi

from norm.sqlite import SqliteSyncTranslator
from norm.common import SyncRunner, AdbapiRunner
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



class SqliteSyncTranslatorTest(TranslateRunnerTestMixin, TestCase):


    def getConnection(self):
        db = sqlite.connect(':memory:')
        db.execute('''create table foo (
            id integer primary key,
            name text
        )''')
        return db


    def getRunner(self, translator):
        return SyncRunner(self.getConnection(), translator)


    def getTranslator(self):
        return SqliteSyncTranslator()


    def test_translateParams(self):
        """
        Should leave ? alone
        """
        trans = SqliteSyncTranslator()
        self.assertEqual(trans.translateParams('select ?'), 'select ?')



class SqliteAdbapiTest(TranslateRunnerTestMixin, TestCase):


    def getRunner(self, translator):
        cpool = adbapi.ConnectionPool(sqlite_module, database=':memory:',
                                      cp_min=1, cp_max=1)
        runner = AdbapiRunner(cpool, translator)
        def setup(x):
            x.execute('''create table foo (
                id integer primary key,
                name text
            )''')
        return cpool.runInteraction(setup).addCallback(lambda _:runner)


    def getTranslator(self):
        return SqliteSyncTranslator()
