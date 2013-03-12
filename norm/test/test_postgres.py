from twisted.trial.unittest import TestCase, SkipTest
from twisted.enterprise import adbapi

from norm.postgres import PostgresSyncTranslator
from norm.common import SyncRunner, AdbapiRunner
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


class PostgresSyncTranslatorTest(TranslateRunnerTestMixin, TestCase):


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


    def getRunner(self):
        return SyncRunner(self.getConnection())


    def getTranslator(self):
        return PostgresSyncTranslator()


    def test_translateParams(self):
        """
        Should make ? into %s
        """
        trans = PostgresSyncTranslator()
        self.assertEqual(trans.translateParams('select ?'), 'select %s')


class PostgresAdbapiTest(TranslateRunnerTestMixin, TestCase):


    def getRunner(self):
        kwargs = getConnArgs()
        cpool = adbapi.ConnectionPool('psycopg2', **kwargs)
        runner = AdbapiRunner(cpool)
        def setup(x):
            x.execute('''create table foo (
                id serial primary key,
                name text
            )''')
        def addClean(_):
            self.addCleanup(self.cleanup, cpool)
        d = cpool.runInteraction(setup)
        d.addCallback(addClean)
        return d.addCallback(lambda _:runner)


    def cleanup(self, pool):
        def drop(x):
            x.execute('drop table foo')
        return pool.runInteraction(drop)


    def getTranslator(self):
        return PostgresSyncTranslator()



