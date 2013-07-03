from twisted.internet import defer
from twisted.python import reflect
from norm.common import BlockingRunner
from norm.uri import parseURI, mkConnStr



def _makeSqlite(parsed):
    try:
        from pysqlite2 import dbapi2 as sqlite
    except ImportError:
        import sqlite3 as sqlite
    connstr = mkConnStr(parsed)
    db = sqlite.connect(connstr)
    runner = BlockingRunner(db)
    return defer.succeed(runner)



def _makePostgres(parsed):
    import psycopg2
    connstr = mkConnStr(parsed)
    db = psycopg2.connect(connstr)
    runner = BlockingRunner(db)
    return defer.succeed(runner)



def makePool(uri):
    parsed = parseURI(uri)
    if parsed['scheme'] == 'sqlite':
        return _makeSqlite(parsed)
    elif parsed['scheme'] == 'postgres':
        return _makePostgres(parsed)
    else:
        raise Exception('%s is not supported' % (parsed['scheme'],))