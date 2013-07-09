# Copyright (c) Matt Haggard.
# See LICENSE for details.

from norm.orm.base import Property



class Int(Property):
    

    def __init__(self, *args, **kwargs):
        Property.__init__(self, *args, **kwargs)
        self.validators.append(self._validate)


    def _validate(self, prop, obj, value):
        if value is None:
            return value
        if type(value) != int:
            raise TypeError('%r must be an integer, not %r' % (prop, value))
        return value