from twisted.trial.unittest import TestCase
from zope.interface.verify import verifyObject


from norm.interface import ITranslator, IRunner
from norm.operation import Insert
from norm.sqlite import SyncTranslator, SyncRunner



class SyncTranslatorTest(TestCase):


    def getConnection(self):
        from pysqlite2 import dbapi2 as sqlite
        db = sqlite.connect(':memory:')
        db.execute('create table foo (id integer primary key)')
        return db


    def test_ITranslator(self):
        verifyObject(ITranslator, SyncTranslator())


    def test_Insert(self):
        """
        You can Insert a record.
        """        
        runner = SyncRunner(self.getConnection())
        translator = SyncTranslator()
        
        translated = translator.translate(Insert('foo'))
        result = runner.run(translated)
        self.assertEqual(result, 1, "Should return the primary key")



class SyncRunnerTest(TestCase):


    def test_IRunner(self):
        verifyObject(IRunner, SyncRunner(None))
