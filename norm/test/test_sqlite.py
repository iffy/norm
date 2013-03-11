from twisted.trial.unittest import TestCase

from norm.sqlite import SyncTranslator, SyncRunner
from norm.test.mixin import TranslateRunnerTestMixin



class SyncTranslatorTest(TranslateRunnerTestMixin, TestCase):


    def getConnection(self):
        from pysqlite2 import dbapi2 as sqlite
        db = sqlite.connect(':memory:')
        #db.row_factory = sqlite.Row
        db.execute('''create table foo (
            id integer primary key,
            name text
        )''')
        return db


    def getRunner(self):
        return SyncRunner(self.getConnection())


    def getTranslator(self):
        return SyncTranslator()

