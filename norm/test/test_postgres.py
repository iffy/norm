from twisted.trial.unittest import TestCase, SkipTest

from norm.postgres import PostgresTranslator
from norm.common import BlockingRunner
from norm.test.mixin import TranslateRunnerTestMixin

from urlparse import urlparse

import os
psycopg2 = None
conn_args = None

try:
    import psycopg2
except ImportError:
    pass


def getConnArgs():
    url = os.environ.get('NORM_POSTGRESQL_URL', None)
    if not url:
        raise SkipTest('You must define NORM_POSTGRESQL_URL in order to do '
                       'testing against a postgres database.  It should be '
                       'in the format user:password@host:port/database')
    parsed = urlparse(url)
    return {
        'user': parsed.username,
        'password': parsed.password,
        'port': parsed.port,
        'host': parsed.hostname,
        'database': parsed.path.lstrip('/')
    }


class PostgresBlockingTest(TranslateRunnerTestMixin, TestCase):


    def getConnection(self):
        kwargs = getConnArgs()
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


    def getRunner(self, translator):
        return BlockingRunner(self.getConnection(), translator)


    def getTranslator(self):
        return PostgresTranslator()


    def doRollback(self, runner):
        runner.conn.rollback()


    def doCommit(self, runner):
        runner.conn.commit()


    def test_translateParams(self):
        """
        Should make ? into %s
        """
        trans = PostgresTranslator()
        self.assertEqual(trans.translateParams('select ?'), 'select %s')


