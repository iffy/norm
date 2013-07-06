# Copyright (c) Matt Haggard.
# See LICENSE for details.

from distutils.core import setup

import os, re

def getVersion():
    r_version = re.compile(r"__version__\s*=\s*'(.*?)'")
    base_init = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'norm/__init__.py')
    guts = open(base_init, 'r').read()
    m = r_version.search(guts)
    if not m:
        raise Exception("Could not find version information")
    return m.groups()[0]


setup(
    url='https://github.com/iffy/norm',
    author='Matt Haggard',
    author_email='haggardii@gmail.com',
    name='norm',
    version=getVersion(),
    packages=[
        'norm', 'norm.test',
    ],
    requires = [
        'Twisted',
    ]
)
