# Copyright (c) Matt Haggard.
# See LICENSE for details.

from collections import defaultdict
import inspect
import weakref



class Property(object):
    """
    I am a property on a class that maps to database column.
    """

    _values = weakref.WeakKeyDictionary()
    _changes = weakref.WeakKeyDictionary()
    attr_name = None
    cls = None


    def __init__(self, column_name=None, fromDatabase=None, toDatabase=None,
                 default_factory=None):
        self.column_name = column_name
        self._fromDatabase = fromDatabase or (lambda x:x)
        self._toDatabase = toDatabase or (lambda x:x)
        self._default_factory = default_factory


    def __get__(self, obj, cls):
        if not self.attr_name:
            self.cacheAttrName(cls)
        if obj:
            values = self.values(obj)
            try:
                return values[self.attr_name]
            except KeyError:
                default = None
                if self._default_factory:
                    default = self._default_factory()
                    self._markChanged(obj)
                return values.setdefault(self.attr_name, default)
        return self


    def __set__(self, obj, val):
        if not self.attr_name:
            self.cacheAttrName(obj.__class__)
        self.values(obj)[self.attr_name] = val
        self._markChanged(obj)


    def values(self, obj):
        return self._values.setdefault(obj, {})


    def valueFor(self, obj):
        """
        Return the python-land value of this property for the given object.
        """
        return self.values(obj).get(self.attr_name, None)


    def _markChanged(self, obj):
        self.changes(obj).append(self)


    def changes(self, obj):
        return self._changes.setdefault(obj, [])


    def cacheAttrName(self, cls):
        self.cls = cls
        for attr in dir(cls):
            try:
                prop = cls.__dict__[attr]
            except:
                continue
            if isinstance(prop, Property):
                prop.attr_name = attr
                prop.column_name = prop.column_name or attr


    def toDatabase(self, obj):
        """
        Get the value of this L{Property} for the given object converted for
        a database.

        @param obj: The obj on which this descriptor lives.
        """
        return self._toDatabase(self.values(obj).get(self.attr_name, None))


    def fromDatabase(self, obj, value):
        """
        Set the value of this attribute as from a database, doing any applicable
        conversions for this attribute.

        @param obj: The obj on which this descriptor lives.
        @param value: The value returned by the database.
        """
        self.values(obj)[self.attr_name] = self._fromDatabase(value)


    def __repr__(self):
        return '%r.%s' % (self.cls, self.attr_name)



class _ClassInfo(object):


    def __init__(self, cls):
        self.cls = cls
        self.columns = defaultdict(lambda:[])
        self.attributes = {}
        self._getInfo()


    def _getInfo(self):
        for k,v in inspect.getmembers(self.cls, lambda x:isinstance(x, Property)):
            self.columns[v.column_name].append(v)
            self.attributes[v.attr_name] = v



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


