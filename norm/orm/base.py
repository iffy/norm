# Copyright (c) Matt Haggard.
# See LICENSE for details.

from collections import defaultdict
import inspect
import weakref



class Property(object):
    """
    I am a property on a class that maps to database column.
    """

    _value_dict = weakref.WeakKeyDictionary()
    _changes = weakref.WeakKeyDictionary()
    attr_name = None
    cls = None
    primary = False


    def __init__(self, column_name=None, primary=False, fromDatabase=None,
                 toDatabase=None, default_factory=None, validators=None):
        self.column_name = column_name
        self._fromDatabase = fromDatabase or (lambda x:x)
        self._toDatabase = toDatabase or (lambda x:x)
        self._default_factory = default_factory
        self.validators = validators or []
        self.primary = primary


    def __get__(self, obj, cls):
        if not self.attr_name:
            self.cacheAttrName(cls)
        if obj:
            return self._getValue(obj)
        return self


    def __set__(self, obj, val):
        self._setValue(obj, val)


    def _setValue(self, obj, value, record_change=True):
        if not self.attr_name:
            self.cacheAttrName(obj.__class__)
        new_value = value
        for v in self.validators:
            new_value = v(self, obj, new_value)
        self._values(obj)[self.attr_name] = new_value
        if record_change:
            self._markChanged(obj)


    def _getValue(self, obj):
        if not self.attr_name:
            self.cacheAttrName(obj.__class__)
        values = self._values(obj)
        try:
            return values[self.attr_name]
        except KeyError:
            if self._default_factory:
                self._setValue(obj, self._default_factory())
            else:
                self._setValue(obj, None, record_change=False)
            return values[self.attr_name]


    def _values(self, obj):
        return self._value_dict.setdefault(obj, {})


    def valueFor(self, obj):
        """
        Return the python-land value of this property for the given object.
        """
        return self._getValue(obj)


    def _markChanged(self, obj):
        self.changes(obj).append(self)


    def changes(self, obj):
        for prop in classInfo(obj.__class__).attributes.values():
            # this is so that default values are populated
            prop.valueFor(obj)
        return self._changes.setdefault(obj, [])


    def cacheAttrName(self, cls):
        for attr in dir(cls):
            try:
                prop = cls.__dict__[attr]
            except:
                continue
            if isinstance(prop, Property):
                prop.attr_name = attr
                prop.column_name = prop.column_name or attr
                prop.cls = cls


    def toDatabase(self, obj):
        """
        Get the value of this L{Property} for the given object converted for
        a database.

        @param obj: The obj on which this descriptor lives.
        """
        return self._toDatabase(self._getValue(obj))


    def fromDatabase(self, obj, value):
        """
        Set the value of this attribute as from a database, doing any applicable
        conversions for this attribute.

        @param obj: The obj on which this descriptor lives.
        @param value: The value returned by the database.
        """
        self._setValue(obj, self._fromDatabase(value), record_change=False)


    def __repr__(self):
        name = self.attr_name
        if self.attr_name != self.column_name:
            name = '%s (aka %s)' % (name, self.column_name)
        return '<Property %s of %r 0x%x>' % (name, self.cls, id(self))



def SQLTable(name):
    """
    Decorator for specifying the sql table 
    """
    def deco(cls):
        _ClassInfo._tables[cls] = name
        return cls
    return deco



class _ClassInfo(object):

    _tables = {}

    def __init__(self, cls):
        self.cls = cls
        self.table = None
        self.columns = defaultdict(lambda:[])
        self.attributes = {}
        self.primaries = []
        self._getInfo()


    def _getInfo(self):
        self.table = self._tables.get(self.cls, None)
        for k,v in inspect.getmembers(self.cls, lambda x:isinstance(x, Property)):
            self.columns[v.column_name].append(v)
            self.attributes[v.attr_name] = v
            if v.primary:
                self.primaries.append(v)



def classInfo(cls):
    # XXX implement caching if it's a big deal
    return _ClassInfo(cls)



class _ObjectInfo(object):


    def __init__(self, obj):
        self.obj = obj


    def changed(self):
        cls_info = classInfo(self.obj.__class__)
        # XXX it's a little weird that you can get to this through any
        # attribute.
        prop = cls_info.attributes.values()[0]
        return prop.changes(self.obj)


    def resetChangedList(self):
        changes = self.changed()
        while changes:
            changes.pop()



def objectInfo(obj):
    """
    XXX
    """
    # XXX implement caching if it's a big deal
    return _ObjectInfo(obj)


