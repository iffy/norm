from twisted.trial.unittest import TestCase, SkipTest


from norm.uri import parseURI, mkConnStr

import os
psycopg2 = None
conn_args = None

try:
    import psycopg2
except ImportError:
    pass


def getConnStr():
    url = os.environ.get('NORM_POSTGRESQL_URL', None)
    if not url:
        raise SkipTest('You must define NORM_POSTGRESQL_URL in order to do '
                       'testing against a postgres database.  It should be '
                       'in the format user:password@host:port/database')
    return mkConnStr(parseURI(url))


