# Copyright (c) Matt Haggard.
# See LICENSE for details.

from twisted.trial.unittest import TestCase


from norm.orm.base import Property
from norm.orm.props import Int



class IntTest(TestCase):


    def test_Property(self):
        """
        It should behave like a property
        """
        self.assertTrue(issubclass(Int, Property))


    def test_int(self):
        """
        The int type will accept ints
        """
        class Foo(object):
            a = Int()


        foo = Foo()
        foo.a = 12
        self.assertEqual(foo.a, 12)


    def test_None(self):
        class Foo(object):
            a = Int()

        foo = Foo()
        foo.a = None
        self.assertEqual(foo.a, None)


    def test_nonInt(self):
        class Foo(object):
            a = Int()


        foo = Foo()
        bads = ['a', [], (), u'\N{SNOWMAN}', {}, True, False, 12.2]
        for b in bads:
            self.assertRaises(TypeError, setattr, foo, 'a', b)
