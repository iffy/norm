from twisted.trial.unittest import TestCase, SkipTest

from norm.postgres import SyncTranslator, SyncRunner
from norm.test.mixin import TranslateRunnerTestMixin


import os
psycopg2 = None
skip_psycopg2_reason = None

try:
    import psycopg2
except ImportError:
    skip_psycopg2_reason = 'psycopg2 not installed'



class SyncTranslatorTest(TranslateRunnerTestMixin, TestCase):

    skip = skip_psycopg2_reason


    def getConnection(self):
        url = os.environ.get('NORM_POSTGRESQL_URL', None)
        if not url:
            raise SkipTest('You must define NORM_POSTGRESQL_URL in order to do '
                           'testing against a postgres database.  It should be '
                           'in the format XXX')
        db = psycopg2.connect(**url)
        c = db.cursor()
        c.execute('''create table foo (
            id serial primary key,
            name text
        )''')
        c.close()
        db.commit()
        return db


    def getRunner(self):
        return SyncRunner(self.getConnection())


    def getTranslator(self):
        return SyncTranslator()


    def test_translateParams(self):
        """
        Should make ? into %s
        """
        trans = SyncTranslator()
        self.assertEqual(trans.translateParams('select ?'), 'select %s')

