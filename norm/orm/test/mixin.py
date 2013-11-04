# Copyright (c) Matt Haggard.
# See LICENSE for details.

from twisted.internet import defer
from zope.interface.verify import verifyObject

from datetime import datetime, date

from norm.interface import IOperator
from norm.orm.props import Int, String, Unicode, Date, DateTime, Bool
from norm.orm.expr import Query, Eq, And, LeftJoin
from norm.orm.error import NotFound
from norm import ormHandle



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
    stars = Int()

    def __init__(self, child_id=None, book_id=None):
        self.child_id = child_id
        self.book_id = book_id


class Book(object):
    __sql_table__ = 'book'
    id = Int(primary=True)
    name = Unicode()

    def __init__(self, name=None):
        self.name = name



class FunctionalIOperatorTestsMixin(object):
    """
    I am a mixin for functionally testing different database IOperators.  To
    use me, you'll need to create a connection pool (runner) and create some
    database tables.  See L{norm.orm.test.test_sqlite} for an example.
    """


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
        self.assertNotEqual(empty.id, None, "Should populate the primary key id")
        self.assertEqual(empty.name, None)
        self.assertEqual(empty.uni, None)
        self.assertEqual(empty.date, None)
        self.assertEqual(empty.dtime, None)
        self.assertEqual(empty.mybool, None)


    @defer.inlineCallbacks
    def test_insert_None(self):
        """
        You can set fields to None
        """
        oper = yield self.getOperator()
        pool = yield self.getPool()
        empty = Empty()
        empty.name = None
        empty.uni = None
        empty.date = None
        empty.dtime = None
        empty.mybool = None

        yield pool.runInteraction(oper.insert, empty)
        self.assertNotEqual(empty.id, None)
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
        self.assertNotEqual(empty.id, None)
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
        self.assertNotEqual(defs.id, None)
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
                                          Query(Empty, Eq(Empty.id, e1.id)))
        self.assertEqual(len(items), 1, "Should return one item")
        self.assertEqual(items[0].name, '1')


    @defer.inlineCallbacks
    def test_query_Eq_str(self):
        """
        You can query by equality of a string
        """
        oper = yield self.getOperator()
        pool = yield self.getPool()

        e1 = Empty()
        e1.name = '1'
        yield pool.runInteraction(oper.insert, e1)

        items = yield pool.runInteraction(oper.query,
                Query(Empty, Eq(Empty.name, '1')))
        self.assertEqual(len(items), 1)


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
    def test_query_LeftJoin(self):
        """
        You can query across two tables with the a LEFT JOIN
        """
        oper = yield self.getOperator()
        pool = yield self.getPool()

        p1 = Parent()
        p1.id = 1
        p2 = Parent()
        p2.id = 2
        c1 = Child(u'child1')
        c1.parent_id = 1

        for obj in [p1, p2, c1]:
            yield pool.runInteraction(oper.insert, obj)
        
        items = yield pool.runInteraction(oper.query,
                Query((Parent,Child), joins=[
                      LeftJoin(Child, Child.parent_id == Parent.id)]))
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0][0].id, 1)
        self.assertEqual(items[0][1].name, 'child1')
        self.assertEqual(items[1][0].id, 2)
        self.assertEqual(items[1][1], None)


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
    def test_query_noConstraints(self):
        """
        You can query for an object with no constraints
        """
        oper = yield self.getOperator()
        pool = yield self.getPool()

        parent = yield pool.runInteraction(oper.insert, Parent())

        query = Query(Parent)
        results = yield pool.runInteraction(oper.query, query)
        self.assertEqual(len(results), 1)
        self.assertTrue(isinstance(results[0], Parent))

        # sub query
        child = Child()
        child.parent_id = parent.id
        yield pool.runInteraction(oper.insert, child)
        query = query.find(Child, Child.parent_id == Parent.id)
        results = yield pool.runInteraction(oper.query, query)
        self.assertEqual(len(results), 1)
        self.assertTrue(isinstance(results[0], Child))

        # left join
        query = Query(Parent).find(Child,
                                   joins=[LeftJoin(Child,
                                          Child.parent_id == Parent.id)])
        results = yield pool.runInteraction(oper.query, query)
        self.assertEqual(len(results), 1)
        self.assertTrue(isinstance(results[0], Child))


    @defer.inlineCallbacks
    def test_query_leftJoin_Parent_toParentChild(self):
        oper = yield self.getOperator()
        pool = yield self.getPool()

        parent = yield pool.runInteraction(oper.insert, Parent())
        child = Child()
        child.parent_id = parent.id
        yield pool.runInteraction(oper.insert, child)

        query = Query(Parent).find((Parent, Child),
                        joins=[LeftJoin(Child, Child.parent_id == Parent.id)])
        results = yield pool.runInteraction(oper.query, query)
        self.assertEqual(len(results), 1)


    @defer.inlineCallbacks
    def test_query_buildLeftJoin(self):
        """
        You can build on to a left-joined query
        """
        oper = yield self.getOperator()
        pool = yield self.getPool()

        
        parent = yield pool.runInteraction(oper.insert, Parent())
        child = Child(u'child')
        child.parent_id = parent.id
        yield pool.runInteraction(oper.insert, child)

        yield pool.runInteraction(oper.insert, Parent())

        book = Book()
        book.name = u'Around the World in 80 Days'
        yield pool.runInteraction(oper.insert, book)

        yield pool.runInteraction(oper.insert,
                                  FavoriteBook(child.id, book.id))
        
        def q(query):
            return pool.runInteraction(oper.query, query)

        # single
        query = Query(Parent, Parent.id != None)
        rows = yield q(query)
        self.assertEqual(len(rows), 2)

        # left join
        query = query.find(Child, joins=[
                           LeftJoin(Child, Child.parent_id == Parent.id)])
        rows = yield q(query)
        self.assertEqual(len(rows), 2)
        self.assertIn(None, rows, "Should have one null record")

        # left join 2
        query = query.find(FavoriteBook, joins=[
                           LeftJoin(FavoriteBook, FavoriteBook.child_id == Child.id)])
        rows = yield q(query)
        self.assertEqual(len(rows), 2)
        self.assertIn(None, rows, "Should have one null record")

        # change select
        query = query.find((Parent, FavoriteBook))
        rows = yield q(query)
        self.assertEqual(len(rows), 2)
        self.assertIn(None, [x[1] for x in rows], "Should have null FavoriteBook")


    @defer.inlineCallbacks
    def test_query_build(self):
        """
        You can build on to a query
        """
        oper = yield self.getOperator()
        pool = yield self.getPool()

        # distractions to make sure we're not picking them up
        p = yield pool.runInteraction(oper.insert, Parent())
        c = Child(u'nope')
        c.parent_id = p.id
        yield pool.runInteraction(oper.insert, c)
        b = Book()
        b.name = u'Gone in Sixty Seconds'
        yield pool.runInteraction(oper.insert, b)

        f = FavoriteBook(c.id, b.id)
        yield pool.runInteraction(oper.insert, f)
        
        # data we're looking for
        parent = yield pool.runInteraction(oper.insert, Parent())
        child = Child(u'child')
        child.parent_id = parent.id
        yield pool.runInteraction(oper.insert, child)

        book = Book()
        book.name = u'Around the World in 80 Days'
        yield pool.runInteraction(oper.insert, book)

        fav = yield pool.runInteraction(oper.insert,
                                        FavoriteBook(child.id, book.id))

        def q(query):
            return pool.runInteraction(oper.query, query)

        # simple
        query = Query(Parent, Eq(Parent.id, parent.id))
        rows = yield q(query)
        self.assertEqual(len(rows), 1, "Should return just the one parent")

        # one join
        query2 = query.find(Child, Eq(Parent.id, Child.parent_id))
        rows = yield q(query2)
        self.assertEqual(len(rows), 1, "Should return just the child")
        row = rows[0]
        self.assertEqual(row.name, 'child')

        # two joins
        query3 = query2.find(FavoriteBook, Eq(Child.id, FavoriteBook.child_id))
        rows = yield q(query3)
        self.assertEqual(len(rows), 1, "Should return just the one favorite book")
        row = rows[0]
        self.assertTrue(isinstance(row, FavoriteBook))
        self.assertEqual(row.child_id, child.id)
        self.assertEqual(row.book_id, fav.book_id)

        # three joins
        query4 = query3.find(Book, Eq(Book.id, FavoriteBook.book_id))
        rows = yield q(query4)
        self.assertEqual(len(rows), 1, "Should return the one book")
        row = rows[0]
        self.assertTrue(isinstance(row, Book))
        self.assertEqual(row.name, u'Around the World in 80 Days')

        # Parent straight to Book
        query5 = query.find(Book,
                    And(
                        Eq(Book.id, FavoriteBook.book_id),
                        Eq(FavoriteBook.child_id, Child.id),
                        Eq(Child.parent_id, Parent.id),
                        Eq(Parent.id, parent.id),
                    ))
        rows = yield q(query5)
        self.assertEqual(len(rows), 1, "Just the one book still")
        row = rows[0]
        self.assertEqual(row.name, u'Around the World in 80 Days')


    @defer.inlineCallbacks
    def test_query_buildLeftJoin_atTheEnd(self):
        """
        You should be able to successfully query
        """
        oper = yield self.getOperator()
        pool = yield self.getPool()
        handle = ormHandle(pool)

        p1 = yield handle.insert(Parent())
        yield handle.insert(Parent())

        c1 = Child()
        c1.parent_id = p1.id
        yield handle.insert(c1)

        c2 = Child()
        c2.parent_id = p1.id
        yield handle.insert(c2)

        b1 = yield handle.insert(Book())
        yield handle.insert(Book())

        yield handle.insert(FavoriteBook(c1.id, b1.id))

        query = Query(Child,
                      Child.parent_id == Parent.id,
                      Parent.id == p1.id)
        query = query.find(Child, Child.id == c1.id)
        query = query.find(FavoriteBook, FavoriteBook.child_id == Child.id)
        query = query.find((FavoriteBook, Book),
                    joins=[LeftJoin(Book,
                           FavoriteBook.book_id == Book.id)])

        rows = yield pool.runInteraction(oper.query, query)
        self.assertEqual(len(rows), 1)
        self.assertTrue(isinstance(rows[0][0], FavoriteBook))
        self.assertEqual(rows[0][0].child_id, c1.id)
        self.assertEqual(rows[0][0].book_id, b1.id)
        self.assertTrue(isinstance(rows[0][1], Book))
        self.assertEqual(rows[0][1].id, b1.id)


    @defer.inlineCallbacks
    def test_query_findChangeSelect(self):
        """
        You can change just the select portion of the query with find
        """
        oper = yield self.getOperator()
        pool = yield self.getPool()

        p = Parent()
        p.id = 2
        yield pool.runInteraction(oper.insert, p)

        c = Child()
        c.id = 4
        c.parent_id = 2
        yield pool.runInteraction(oper.insert, c)

        query = Query(Child, Parent.id == Child.parent_id,
                             Parent.id == p.id)
        items = yield pool.runInteraction(oper.query, query)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, 4)

        query = query.find(Parent)
        items = yield pool.runInteraction(oper.query, query)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, 2)


    @defer.inlineCallbacks
    def test_query_And_default(self):
        """
        Constraints are Anded together by default (without needing to include
        C{And})
        """
        oper = yield self.getOperator()
        pool = yield self.getPool()
        
        e1 = Empty()
        e1.name = '1'
        yield pool.runInteraction(oper.insert, e1)

        e2 = Empty()
        e2.name = '2'
        yield pool.runInteraction(oper.insert, e2)

        query = Query(Empty, Empty.name == '1', Empty.id != None)
        items = yield pool.runInteraction(oper.query, query)
        self.assertEqual(len(items), 1)
        item = items[0]
        
        self.assertTrue(isinstance(item, Empty))
        self.assertEqual(item.name, '1')


    @defer.inlineCallbacks
    def test_refresh(self):
        """
        You can get an object by id
        """
        oper = yield self.getOperator()
        pool = yield self.getPool()

        obj = Empty()
        obj.name = 'hello'
        obj = yield pool.runInteraction(oper.insert, obj)

        fresh = Empty()
        fresh.id = obj.id

        obj2 = yield pool.runInteraction(oper.refresh, fresh)
        self.assertEqual(obj2, fresh, "Should return the same instance")
        self.assertEqual(obj2.name, 'hello', "Should update attributes")


    @defer.inlineCallbacks
    def test_refresh_multiPrimary(self):
        """
        You can get an object by compound primary key
        """
        oper = yield self.getOperator()
        pool = yield self.getPool()

        obj = FavoriteBook(3, 12)
        obj.stars = 800
        yield pool.runInteraction(oper.insert, obj)

        fresh = FavoriteBook(3, 12)
        obj2 = yield pool.runInteraction(oper.refresh, fresh)
        self.assertEqual(obj2, fresh)
        self.assertEqual(obj2.stars, 800)



    @defer.inlineCallbacks
    def test_update(self):
        """
        You can update a row using an object
        """
        oper = yield self.getOperator()
        pool = yield self.getPool()

        obj = Empty()
        yield pool.runInteraction(oper.insert, obj)

        obj.name = 'new name'
        obj.uni = u'unicycle'
        obj.date = date(2000, 1, 1)
        yield pool.runInteraction(oper.update, obj)

        obj2 = Empty()
        obj2.id = obj.id
        yield pool.runInteraction(oper.refresh, obj2)
        self.assertEqual(obj2.name, 'new name')
        self.assertEqual(obj2.uni, u'unicycle')
        self.assertEqual(obj2.date, date(2000, 1, 1))


    @defer.inlineCallbacks
    def test_delete(self):
        """
        You can delete single objects
        """
        oper = yield self.getOperator()
        pool = yield self.getPool()

        obj = yield pool.runInteraction(oper.insert, Empty())

        yield pool.runInteraction(oper.delete, obj)

        obj2 = Empty()
        obj2.id = obj.id
        self.assertFailure(pool.runInteraction(oper.refresh, obj2), NotFound)


