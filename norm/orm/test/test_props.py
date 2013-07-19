# Copyright (c) Matt Haggard.
# See LICENSE for details.

from twisted.trial.unittest import TestCase

from datetime import date, datetime

from norm.orm.base import Property
from norm.orm.props import Int, Bool, Date, DateTime, String, Unicode



class PropertyTestMixin(object):

    property_class = None
    good_values = []
    bad_values = []


    def test_Property_Property(self):
        self.assertTrue(issubclass(self.property_class, Property))


    def test_Property_goodValues(self):
        class Foo(object):
            a = self.property_class()


        foo = Foo()
        for value in self.good_values:
            foo.a = value
            self.assertEqual(foo.a, value)


    def test_Property_badValues(self):
        class Foo(object):
            a = self.property_class()


        foo = Foo()
        for value in self.bad_values:
            try:
                foo.a = value
            except TypeError:
                # success
                pass
            else:
                self.fail("It should be a TypeError to set %s to %r" % (
                    self.property_class, value))




class IntTest(PropertyTestMixin, TestCase):

    property_class = Int
    good_values = [None, 1, -1, 0, 10, 12L]
    bad_values = ['a', u'b', [], (), {}, True, False, 12.2]



class BoolTest(PropertyTestMixin, TestCase):

    property_class = Bool
    good_values = [None, True, False, 1, 0]
    bad_values = ['a', u'b', [], (), {}, 12.2]



class DateTest(PropertyTestMixin, TestCase):

    property_class = Date
    good_values = [None, date(2001, 1, 2)]
    bad_values = ['a', datetime(2001, 1, 1), [], (), False, 1, 12.1]



class DateTimeTest(PropertyTestMixin, TestCase):

    property_class = DateTime
    good_values = [None, datetime(2001, 12, 1)]
    bad_values = ['a', date(2001, 1, 1), [], (), False, 1, 12.1]



class StringTest(PropertyTestMixin, TestCase):

    property_class = String
    good_values = [None, 'foo', u'\N{SNOWMAN}'.encode('utf-8')]
    bad_values = [u'foo', date(2000, 1, 1), [], {}, False, 1, 12.1]


class UnicodeTest(PropertyTestMixin, TestCase):

    property_class = Unicode
    good_values = [None, u'foo', u'\N{SNOWMAN}', 'foo'.decode('utf-8')]
    bad_values = ['foo', 12, False, [], {}, object()]


