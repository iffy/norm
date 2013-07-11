# Copyright (c) Matt Haggard.
# See LICENSE for details.

from norm.orm.base import Property

from datetime import date, datetime



class Int(Property):
    

    def _validate(self, prop, obj, value):
        if value is None:
            return value
        if type(value) not in (int, long):
            raise TypeError('%r must be an integer, not %r' % (prop, value))
        return value



class Bool(Property):


    def _validate(self, prop, obj, value):
        if value is None:
            return None
        if type(value) not in (bool, int):
            raise TypeError('%r must be a boolean, not %r' % (prop, value))
        return bool(value)



class Date(Property):


    def _validate(self, prop, obj, value):
        if type(value) not in (type(None), date):
            raise TypeError('%r must be a date, not %r' % (prop, value))
        return value



class DateTime(Property):


    def _validate(self, prop, obj, value):
        if type(value) not in (type(None), datetime):
            raise TypeError('%r must be a datetime, not %r' % (prop, value))
        return value



class String(Property):


    def _validate(self, prop, obj, value):
        if type(value) not in (type(None), str):
            raise TypeError('%r must be a str, not %r' % (prop, value))
        return value



class Unicode(Property):


    def _validate(self, prop, obj, value):
        if type(value) not in (type(None), unicode):
            raise TypeError('%r must be a unicode, not %r' % (prop, value))
        return value