# Copyright (c) Matt Haggard.
# See LICENSE for details.

from twisted.trial.unittest import TestCase
from twisted.internet import defer
from zope.interface.verify import verifyObject

from datetime import datetime, date

from norm.interface import IOperator
from norm.patch import Patcher
from norm.porcelain import makePool
from norm.sqlite import SqliteOperator
from norm.orm.props import Int, String, Unicode, Date, DateTime, Bool
from norm.orm.expr import Query, Eq, And



class Empty(object):
    __sql_table__ = 'empty'
    id = Int(primary=True)
    name = String()
    uni = Unicode()
    date = Date()
    dtime = DateTime()
    mybool = Bool()


class Defaults(object):
    __sql_table__ = 'with_defaults'
    id = Int(primary=True)
    name = String()
    uni = Unicode()
    date = Date()
    dtime = DateTime()
    mybool = Bool()


class Parent(object):
    __sql_table__ = 'parent'
    id = Int(primary=True)
    name = Unicode()


class Child(object):
    __sql_table__ = 'child'
    id = Int(primary=True)
    name = Unicode()
    parent_id = Int()

    def __init__(self, name=None):
        self.name = name


class FavoriteBook(object):
    __sql_table__ = 'favorite_book'
    child_id = Int(primary=True)
    book_id = Int(primary=True)

    def __init__(self, child_id=None, book_id=None):
        self.child_id = child_id
        self.book_id = book_id


class Book(object):
    __sql_table__ = 'book'
    id = Int(primary=True)
    name = Unicode()

    def __init__(self, name=None):
        self.name = name



class CommonTestsMixin(object):


    def getOperator(self):
        raise NotImplementedError("You must implement getOperator to use this "
                                  "mixin")


    def getPool(self):
        raise NotImplementedError("You must implement getPool to use this "
                                  "mixin")


    @defer.inlineCallbacks
    def test_IOperator(self):
        oper = yield self.getOperator()
        verifyObject(IOperator, oper)


    @defer.inlineCallbacks
    def test_insert_noValues(self):
        """
        You can insert into the database.
        """
        oper = yield self.getOperator()
        pool = yield self.getPool()
        empty = yield pool.runInteraction(oper.insert, Empty())
        self.assertTrue(isinstance(empty, Empty), "Should return an instance"
                        " of Empty, not %r" % (empty,))
        self.assertEqual(empty.id, 1, "Should populate the primary key id")
        self.assertEqual(empty.name, None)
        self.assertEqual(empty.uni, None)
        self.assertEqual(empty.date, None)
        self.assertEqual(empty.dtime, None)
        self.assertEqual(empty.mybool, None)


    @defer.inlineCallbacks
    def test_insert_values(self):
        """
        You can insert into the database with some values
        """
        oper = yield self.getOperator()
        pool = yield self.getPool()
        empty = Empty()
        empty.name = 'foo'
        empty.uni = u'something'
        empty.date = date(2000, 1, 1)
        empty.dtime = datetime(2000, 1, 1, 12, 23, 22)
        empty.mybool = True

        yield pool.runInteraction(oper.insert, empty)
        self.assertEqual(empty.id, 1)
        self.assertEqual(empty.name, 'foo')
        self.assertEqual(empty.uni, u'something')
        self.assertEqual(empty.date, date(2000, 1, 1))
        self.assertEqual(empty.dtime, datetime(2000, 1, 1, 12, 23, 22))
        self.assertEqual(empty.mybool, True)


    @defer.inlineCallbacks
    def test_insert_defaults(self):
        """
        After inserting, the object should have the default values from the
        database on it.
        """
        oper = yield self.getOperator()
        pool = yield self.getPool()
        defs = Defaults()

        yield pool.runInteraction(oper.insert, defs)
        self.assertEqual(defs.id, 1)
        self.assertEqual(defs.name, 'hey')
        self.assertEqual(defs.uni, u'ho')
        self.assertEqual(defs.date, date(2001, 1, 1))
        self.assertEqual(defs.dtime, datetime(2001, 1, 1, 12, 22, 32))
        self.assertEqual(defs.mybool, True)


    @defer.inlineCallbacks
    def test_insert_pythonOverrideDefaults(self):
        """
        Explicitly-set python values should override any database defaults
        """
        oper = yield self.getOperator()
        pool = yield self.getPool()
        defs = Defaults()

        defs.name = 'something'
        defs.mybool = False
        yield pool.runInteraction(oper.insert, defs)
        self.assertEqual(defs.mybool, False)
        self.assertEqual(defs.name, 'something')


    @defer.inlineCallbacks
    def test_insert_binary(self):
        """
        Should handle binary data okay
        """
        oper = yield self.getOperator()
        pool = yield self.getPool()
        empty = Empty()
        empty.name = '\x00\x01\x02hey\x00'

        yield pool.runInteraction(oper.insert, empty)
        self.assertEqual(empty.name, '\x00\x01\x02hey\x00')


    @defer.inlineCallbacks
    def test_query_basic(self):
        """
        A basic query should work
        """
        oper = yield self.getOperator()
        pool = yield self.getPool()
        
        e1 = Empty()
        e1.name = '1'
        yield pool.runInteraction(oper.insert, e1)

        e2 = Empty()
        e2.name = '2'
        yield pool.runInteraction(oper.insert, e2)

        items = yield pool.runInteraction(oper.query, Query(Empty))
        self.assertEqual(len(items), 2)
        items = sorted(items, key=lambda x:x.name)
        
        self.assertTrue(isinstance(items[0], Empty))
        self.assertEqual(items[0].name, '1')

        self.assertTrue(isinstance(items[1], Empty))
        self.assertEqual(items[1].name, '2')


    @defer.inlineCallbacks
    def test_query_Eq(self):
        oper = yield self.getOperator()
        pool = yield self.getPool()

        e1 = Empty()
        e1.name = '1'
        yield pool.runInteraction(oper.insert, e1)

        e2 = Empty()
        e2.name = '2'
        yield pool.runInteraction(oper.insert, e2)

        items = yield pool.runInteraction(oper.query,
                                          Query(Empty, Eq(Empty.id, 1)))
        self.assertEqual(len(items), 1, "Should return one item")
        self.assertEqual(items[0].name, '1')


    @defer.inlineCallbacks
    def test_query_autoJoin(self):
        """
        You can query across two tables with the default SQL join
        """
        oper = yield self.getOperator()
        pool = yield self.getPool()

        p1 = Parent()
        p1.id = 1
        p2 = Parent()
        p2.id = 2
        c1 = Child(u'child1')
        c1.parent_id = 1
        c2 = Child(u'child2')
        c2.parent_id = 2

        for obj in [p1, p2, c1, c2]:
            yield pool.runInteraction(oper.insert, obj)
        
        items = yield pool.runInteraction(oper.query,
                Query(Child, And(
                    Eq(Child.parent_id, Parent.id),
                    Eq(Parent.id,1))))
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].name, 'child1', "Should return the one child")


    @defer.inlineCallbacks
    def test_query_multiClass(self):
        """
        You can get two objects at once
        """
        oper = yield self.getOperator()
        pool = yield self.getPool()

        p1 = Parent()
        p1.id = 1
        p2 = Parent()
        p2.id = 2
        c1 = Child(u'child1')
        c1.parent_id = 1
        c2 = Child(u'child2')
        c2.parent_id = 2

        for obj in [p1, p2, c1, c2]:
            yield pool.runInteraction(oper.insert, obj)

        items = yield pool.runInteraction(oper.query,
                Query((Child, Parent), And(
                    Eq(Child.parent_id, Parent.id),
                    Eq(Parent.id, 2))))

        self.assertEqual(len(items), 1)
        child, parent = items[0]
        self.assertTrue(isinstance(child, Child))
        self.assertEqual(child.name, 'child2')
        self.assertTrue(isinstance(parent, Parent))
        self.assertEqual(parent.id, 2)


    @defer.inlineCallbacks
    def test_query_build(self):
        """
        You can build on to a query
        """
        oper = yield self.getOperator()
        pool = yield self.getPool()




class SqliteOperatorTest(CommonTestsMixin, TestCase):

    patcher = Patcher()
    patcher.add('only', [
        '''CREATE TABLE empty (
            id INTEGER PRIMARY KEY,
            name blob,
            uni text,
            date date,
            dtime timestamp,
            mybool tinyint
        )''',
        '''CREATE TABLE with_defaults (
            id INTEGER PRIMARY KEY,
            name blob default 'hey',
            uni text default 'ho',
            date date default '2001-01-01',
            dtime timestamp default '2001-01-01 12:22:32',
            mybool tinyint default 1
        )''',
        '''CREATE TABLE parent (
            id INTEGER PRIMARY KEY,
            name TEXT
        )''',
        '''CREATE TABLE child (
            id INTEGER PRIMARY KEY,
            name TEXT,
            parent_id INTEGER
        )''',
        '''CREATE TABLE favorite_book (
            child_id INTEGER,
            book_id INTEGER,
            PRIMARY KEY (child_id, book_id)
        )''',
        '''CREATE TABLE book (
            id INTEGER PRIMARY KEY,
            name text
        )''',
    ])


    def getOperator(self):
        return SqliteOperator()


    @defer.inlineCallbacks
    def getPool(self):
        pool = yield makePool('sqlite:')
        yield self.patcher.upgrade(pool)
        defer.returnValue(pool)

