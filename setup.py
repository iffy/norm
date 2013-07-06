# Copyright (c) Matt Haggard.
# See LICENSE for details.

from distutils.core import setup


setup(
    url='https://github.com/iffy/norm',
    author='Matt Haggard',
    author_email='haggardii@gmail.com',
    name='norm',
    version='1.1',
    packages=[
        'norm', 'norm.test',
    ],
    requires = [
        'Twisted',
    ]
)
