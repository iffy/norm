# Copyright (c) Matt Haggard.
# See LICENSE for details.

from collections import defaultdict
import inspect
import weakref



class Property(object):
    """
    I am a property on a class that maps to database column.

    @ivar attr_name: Name of the attribute on the class
    @ivar column_name: Name of the column in the database.
    @ivar primary: C{True} if I am a part of the primary key.
    @ivar cls: The Class I live on.
    """

    _value_dict = weakref.WeakKeyDictionary()
    _changes = weakref.WeakKeyDictionary()
    attr_name = None
    cls = None
    primary = False


    def __init__(self, column_name=None, primary=False, fromDatabase=None,
                 toDatabase=None, default_factory=None, validators=None):
        """
        @param column_name: Name of the column in the database if different
            than the name being used in the class.  Meaning, these two lines
            have an equivalent C{column_name}:

                foo = Property()
                foo = Property('foo')

        @param primary: C{True} means this column is part of the primary key
        @param fromDatabase: A function to be called to convert a value from
            the database into one ready for python use.  This should be the
            inverse of C{toDatabase}.
        @param toDatabase: A function that will be called to convert a value
            from python-land to database-land.  This should be the inverse
            of C{fromDatabase}.
        @param default_factory: A function accepting no arguments that will
            be called to compute a default value for this attribute.
        @param validators: A list of functions to validate values being
            assigned to this attribute.  Each function will be called with
            three arguments:
                - prop: (me, the thing you're instantiating with this __init__)
                - obj: The object on which the value is being set
                - value: The value being set.
            Each validator will be called in the order given.
        """
        self.column_name = column_name
        self._fromDatabase = fromDatabase or (lambda x:x)
        self._toDatabase = toDatabase or (lambda x:x)
        self._default_factory = default_factory
        self.validators = validators or []
        self.validators.append(self._validate)
        self.primary = primary


    def __get__(self, obj, cls):
        if not self.attr_name:
            self._cacheAttrName(cls)
        if obj:
            return self._getValue(obj)
        return self


    def __set__(self, obj, val):
        self._setValue(obj, val)


    def _setValue(self, obj, value, record_change=True):
        if not self.attr_name:
            self._cacheAttrName(obj.__class__)
        new_value = value
        for v in self.validators:
            new_value = v(self, obj, new_value)
        self._values(obj)[self.attr_name] = new_value
        if record_change:
            self._markChanged(obj)


    def _getValue(self, obj):
        if not self.attr_name:
            self._cacheAttrName(obj.__class__)
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


    def _validate(self, prop, obj, value):
        """
        Override this in subclasses for default validation.
        """
        return value


    def valueFor(self, obj):
        """
        Return the python-land value of this property for the given object.
        """
        return self._getValue(obj)


    def _markChanged(self, obj):
        self.changes(obj).append(self)


    def changes(self, obj):
        """
        Get a list of changes on this object (not just for this property).
        """
        for prop in classInfo(obj.__class__).attributes.values():
            # this is so that default values are populated
            prop.valueFor(obj)
        return self._changes.setdefault(obj, [])


    def _cacheAttrName(self, cls):
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

        @return: A database-ready value
        """
        return self._toDatabase(self._getValue(obj))


    def fromDatabase(self, obj, value):
        """
        Set the value of this attribute as from a database, doing any applicable
        conversions for this attribute.

        Setting the value of an attribute using me will reset the changed status
        of the attribute.

        @param obj: The obj on which this descriptor lives.
        @param value: The value returned by the database.
        """
        self._setValue(obj, self._fromDatabase(value), record_change=False)
        try:
            self.changes(obj).remove(self)
        except:
            pass


    def __repr__(self):
        name = self.attr_name
        if self.attr_name != self.column_name:
            name = '%s (aka %s)' % (name, self.column_name)
        return '<Property %s of %r 0x%x>' % (name, self.cls, id(self))



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
        self.table = getattr(self.cls, '__sql_table__', None)
        for k,v in inspect.getmembers(self.cls, lambda x:isinstance(x, Property)):
            self.columns[v.column_name].append(v)
            self.attributes[v.attr_name] = v
            if v.primary:
                self.primaries.append(v)



def classInfo(cls):
    # XXX implement caching if it's a big deal
    return _ClassInfo(cls)



class _ObjectInfo(object):
    """
    I am ORM-related information about an object.
    """


    def __init__(self, obj):
        self.obj = obj


    def changed(self):
        """
        Get a list of properties on my object that have changed.
        """
        cls_info = classInfo(self.obj.__class__)
        # XXX it's a little weird that you can get to this through any
        # attribute.
        prop = cls_info.attributes.values()[0]
        return prop.changes(self.obj)


    def resetChangedList(self):
        """
        Reset the changed status of all properties on my object so that an
        immediate call to L{changed} will return an empty list.
        """
        changes = self.changed()
        while changes:
            changes.pop()



def objectInfo(obj):
    """
    Get ORM-related information about an object.  See L{_ObjectInfo} for more
    details.
    """
    # XXX implement caching if it's a big deal
    return _ObjectInfo(obj)



def reconstitute(data):
    """
    Reconstitute an object or list of objects using data from a database.
    This will not call the C{__init__} method of the reconstituted classes.

    @param data: An iterable of 2-tuples (property, value) to be used to set
        the values on the returned instance(s).

    @return: XXX 'spain this, Lucy
    """
    classes = []
    class_data = defaultdict(lambda:[])
    for prop, value in data:
        if prop.cls not in classes:
            classes.append(prop.cls)
        class_data[prop.cls].append((prop, value))
    
    ret = []
    for cls in classes:
        obj = cls.__new__(cls)
        ret.append(obj)
        for prop, value in class_data[prop.cls]:
            prop.fromDatabase(obj, value)

    return ret[0]



class Converter(object):
    """
    I let you register conversion functions for types, then use the conversion
    functions later by passing in the type.  See my tests for usage.
    """

    def __init__(self):
        self.converters = defaultdict(lambda: [])


    def when(self, key):
        """
        """
        def deco(f):
            self.converters[key].append(f)
            return f
        return deco


    def convert(self, key, value):
        """
        XXX
        """
        for conv in self.converters[key]:
            value = conv(value)
        return value








