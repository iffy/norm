from twisted.trial.unittest import TestCase, SkipTest

from norm.postgres import SyncTranslator, SyncRunner
from norm.test.mixin import TranslateRunnerTestMixin

from urlparse import urlparse

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
        parsed = urlparse(url)
        kwargs = {
            'user': parsed.username,
            'password': parsed.password,
            'port': parsed.port,
            'host': parsed.hostname,
            'database': parsed.path.lstrip('/')
        }
        db = psycopg2.connect(**kwargs)
        c = db.cursor()
        c.execute('''create table foo (
            id serial primary key,
            name text
        )''')
        self.addCleanup(self.cleanup, db)
        c.close()
        db.commit()
        return db


    def cleanup(self, db):
        db.rollback()
        c = db.cursor()
        c.execute('drop table foo')
        db.commit()
        db.close()


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

