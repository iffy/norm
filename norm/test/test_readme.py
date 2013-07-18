# Copyright (c) Matt Haggard.
# See LICENSE for details.

from twisted.trial.unittest import TestCase
from twisted.internet import defer
from twisted.python.filepath import FilePath
from twisted.python import log
from twisted.internet.utils import getProcessOutputAndValue

import re
import sys
import os
import textwrap



class ExampleTest(TestCase):

    readme = FilePath(__file__).parent().parent().parent().child('README.md')
    r_snippet = re.compile(r'''
        <!---\s*test\s*-->   # start 
        \s*```python\s*
        (.*?)           # content
        \s*```
        ''', re.I | re.S | re.X)


    def t(self, filepath, name):
        print 'testing', name
        env = os.environ.copy()
        env['PYTHONPATH'] =  self.readme.parent().path + ':' + env.get('PYTHONPATH', '')
        d = getProcessOutputAndValue(sys.executable, [filepath.path],
                                     env=env)
        def check((stdout, stderr, code)):
            log.msg(name)
            log.msg(filepath.path)
            log.msg('stdout: %r' % (stdout,))
            log.msg('stderr: %r' % (stderr,))
            log.msg('exit: %r' % (code,))
            if code != 0:
                raise Exception('\n' + '-'*30 + '\n' + name + ':\n' + stderr)                
        return d.addCallback(check)



    def test_README(self):
        """
        Run the snippets in the README
        """
        tmpdir = FilePath(self.mktemp())
        tmpdir.makedirs()

        # parse the examples out
        guts = self.readme.getContent()
        groups = self.r_snippet.findall(guts)

        dlist = []
        for i, content in enumerate(groups, 0):
            tmpfile = tmpdir.child('test%d.py' % (i,))
            tmpfile.setContent(textwrap.dedent(content))
            dlist.append(self.t(tmpfile, 'code block %s' % i))
        return defer.gatherResults(dlist)
        
    