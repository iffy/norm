# Copyright (c) Matt Haggard.
# See LICENSE for details.

from collections import defaultdict
import inspect
import weakref

from zope.interface import implements

from norm.orm.error import NotFound
from norm.interface import IOperator



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
        if self not in self.changes(obj):
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
        for prop, value in class_data[cls]:
            prop.fromDatabase(obj, value)

    if len(ret) == 1:
        return ret[0]
    return ret


def updateObjectFromDatabase(data, obj, converter):
    """
    Update an existing object's attributes from a database response row.

    @param data: A tuple-dict thing as returned by IAsyncCursor.fetchOne
    @param obj: An ORM'd object
    @param converter: A L{Converter} instance that knows how to convert from
        database-land to python-land.

    @return: The same C{obj} with updated attributes.
    """
    if data is None:
        raise NotFound(obj)
    for name, props in classInfo(obj.__class__).columns.items():
        if name not in data.keys():
            continue
        for prop in props:
            value = converter.convert(prop.__class__, data[name])
            prop.fromDatabase(obj, value)
    return obj



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



class BaseOperator(object):
    """
    I provide asynchronous CRUD database access with objects.
    """

    implements(IOperator)

    compiler = None
    fromDB = None
    toDB = None


    def insert(self, cursor, obj):
        raise NotImplementedError('Implement insert')


    def _makeObjects(self, rows, query):
        """
        Reconstitute objects based on a query and the rows returned from the 
        database.

        @param rows: Rows returned from the database.
        @oaram query: L{Query} used to find these rows.

        @return: A list of reconstituted rows.
        """
        ret = []
        props = query.properties()
        for row in rows:
            data = zip(props, row)
            data = [(x[0], self.fromDB.convert(x[0].__class__, x[1])) for x in data]
            ret.append(reconstitute(data))
        return ret


    def _updateObject(self, data, obj):
        return updateObjectFromDatabase(data, obj, self.fromDB)


    def query(self, cursor, query):
        """
        Query for objects.

        @param query: A L{Query} instance.
        """
        sql, args = self.compiler.compile(query)
        d = cursor.execute(sql, tuple(args))
        d.addCallback(lambda _: cursor.fetchall())
        d.addCallback(self._makeObjects, query)
        return d


    def refresh(self, cursor, obj):
        """
        Update an objects attributes from the values in the database.

        @param obj: Object to update
        """
        info = classInfo(obj.__class__)
        
        args = []
        
        where_parts = []
        for prop in info.primaries:
            where_parts.append('%s=?' % (prop.column_name,))
            args.append(self.toDB.convert(prop.__class__, prop.toDatabase(obj)))
        
        columns = info.columns.keys()
        select = 'SELECT %s FROM %s WHERE %s' % (','.join(columns),
                  info.table, ' AND '.join(where_parts))

        d = cursor.execute(select, tuple(args))
        d.addCallback(lambda _: cursor.fetchone())
        d.addCallback(self._updateObject, obj)
        return d


    def update(self, cursor, obj):
        """
        Update the database from changed attributes on an object.

        @param obj: Object to get attributes for updating from.
        """
        obj_info = objectInfo(obj)
        info = classInfo(obj.__class__)
        changed = obj_info.changed()

        # XXX I copied and modified this from insert
        set_parts = []
        set_args = []
        for prop in changed:
            set_parts.append('%s=?' % (prop.column_name,))
            value = self.toDB.convert(prop.__class__, prop.toDatabase(obj))
            set_args.append(value)

        # XXX I copied this from refresh
        # XXX REFACTOR
        where_parts = []
        where_args = []
        for prop in info.primaries:
            where_parts.append('%s=?' % (prop.column_name,))
            where_args.append(self.toDB.convert(prop.__class__, prop.toDatabase(obj)))

        update = 'UPDATE %s SET %s WHERE %s' % (info.table,
                 ','.join(set_parts),
                 ' AND '.join(where_parts))
        args = tuple(set_args + where_args)

        return cursor.execute(update, args)


    def delete(self, cursor, obj):
        """
        Delete an object from the database.

        @param obj: Object to delete.
        """
        info = classInfo(obj.__class__)

        # XXX I copied this from refresh
        # XXX REFACTOR
        where_parts = []
        where_args = []
        for prop in info.primaries:
            where_parts.append('%s=?' % (prop.column_name,))
            where_args.append(self.toDB.convert(prop.__class__, prop.toDatabase(obj)))

        delete = 'DELETE FROM %s WHERE %s' % (info.table,
                 ' AND '.join(where_parts))

        args = tuple(where_args)

        return cursor.execute(delete, args)




