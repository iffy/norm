# Copyright (c) Matt Haggard.
# See LICENSE for details.

from twisted.trial.unittest import TestCase


from norm.uri import parseURI, mkConnStr


class parseURITest(TestCase):


    def test_sqlite(self):
        """
        sqlite URIs should be supported
        """
        parsed = parseURI('sqlite:')
        self.assertEqual(parsed['scheme'], 'sqlite')
        self.assertEqual(parsed['file'], '')

        parsed = parseURI('sqlite:/tmp/foo')
        self.assertEqual(parsed['scheme'], 'sqlite')
        self.assertEqual(parsed['file'], '/tmp/foo')

        parsed = parseURI('sqlite:tmp/foo')
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



class makeConnStrTest(TestCase):


    def t(self, i, expected):
        parsed = parseURI(i)
        output = mkConnStr(parsed)
        self.assertEqual(output, expected, "Expected URI %r to become conn "
                         "string\n%r\nbut it was\n%r" % (i, expected, output))

    def test_sqlite(self):
        self.t('sqlite:', ':memory:')
        self.t('sqlite:/tmp/foo', '/tmp/foo')
        self.t('sqlite:tmp/foo', 'tmp/foo')


    def test_postgres(self):
        def t(i, expected):
            expected_parts = expected.split(' ')
            parsed = parseURI(i)
            output = mkConnStr(parsed)
            parts = output.split(' ')
            self.assertEqual(set(expected_parts), set(parts),
                             "Expected URI\n    %r\nto become like conn "
                             "string\n    %r\nbut it was\n    %r" % (i,
                             expected, output))
        t('postgres:///postgres', 'dbname=postgres')
        t('postgres://host/postgres', 'dbname=postgres host=host')
        t('postgres://user@host/postgres',
            'dbname=postgres user=user host=host')
        t('postgres://user:pass@host:1234/foo?sslmode=require',
            'dbname=foo user=user password=pass host=host port=1234 sslmode=require')

