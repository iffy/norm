# Copyright (c) Matt Haggard.
# See LICENSE for details.

from twisted.trial.unittest import SkipTest

from norm.uri import mkConnStr, parseURI
import os


postgres_url = os.environ.get('NORM_POSTGRESQL_URI', None)
skip_postgres = ('You must define NORM_POSTGRESQL_URI in order to run this '
                 'postgres test')
if postgres_url:
    skip_postgres = ''


def postgresConnStr():
    if not postgres_url:
        raise SkipTest(skip_postgres)
    return mkConnStr(parseURI(postgres_url))