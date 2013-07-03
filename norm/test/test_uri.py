from twisted.trial.unittest import TestCase


from norm.uri import parseURI


class parseURITest(TestCase):


    def test_sqlite(self):
        """
        sqlite URIs should be supported
        """
        parsed = parseURI('sqlite://')
        self.assertEqual(parsed['scheme'], 'sqlite')
        self.assertEqual(parsed['file'], '')

        parsed = parseURI('sqlite:///tmp/foo')
        self.assertEqual(parsed['scheme'], 'sqlite')
        self.assertEqual(parsed['file'], '/tmp/foo')

        parsed = parseURI('sqlite://tmp/foo')
        self.assertEqual(parsed['scheme'], 'sqlite')
        self.assertEqual(parsed['file'], 'tmp/foo')


    def test_postgres(self):
        """
        Postgres URIs should be supported
        """
        p = parseURI('postgres://')
        self.assertEqual(p['scheme'], 'postgres')

        p = parseURI('postgres:///postgres')
        self.assertEqual(p['db'], 'postgres')
        self.assertFalse('user' in p)
        self.assertFalse('host' in p)
        self.assertFalse('port' in p)
        self.assertFalse('password' in p)
        self.assertFalse('sslmode' in p)

        p = parseURI('postgres://user@host:1234/postgres')
        self.assertEqual(p['db'], 'postgres')
        self.assertEqual(p['user'], 'user')
        self.assertEqual(p['host'], 'host')
        self.assertEqual(p['port'], 1234)

        p = parseURI('postgres://user:password@')
        self.assertEqual(p['user'], 'user')
        self.assertEqual(p['password'], 'password')

        p = parseURI('postgres:///postgres?sslmode=require')
        self.assertEqual(p['db'], 'postgres')
        self.assertEqual(p['sslmode'], 'require')
