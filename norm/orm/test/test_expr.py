# Copyright (c) Matt Haggard.
# See LICENSE for details.

from twisted.trial.unittest import TestCase

from datetime import date, datetime

from norm.orm.base import Property
from norm.orm.expr import (Compiler, State, CompileError, Comparison,
                           Eq, Neq, And, Or, Join, Table, Lt, Lte, Gt, Gte,
                           compiler as base_compiler)



class StateTest(TestCase):


    def test_init(self):
        state = State()
        self.assertEqual(state.compiler, None)


    def test_compile(self):
        """
        Should call through to compiler with the compiler as a second arg
        """
        called = []
        class Fake(object):
            def compile(self, what, state):
                called.append((what, state))
                return 'foo'

        fake = Fake()
        state = State()
        state.compiler = fake
        r = state.compile('something')
        self.assertEqual(r, 'foo')
        self.assertEqual(called, [('something', state)])


    def test_tableAlias(self):
        """
        Should generate a new alias per class, but return the same alias for
        the same class
        """
        class Foo(object):
            __sql_table__ = 'foo'
        class Bar(object):
            __sql_table__ = 'bar'

        state = State()
        self.assertEqual(state.tableAlias(Foo), 'a')
        self.assertEqual(state.tableAlias(Bar), 'b')
        self.assertEqual(state.tableAlias(Foo), 'a')
        self.assertEqual(state.tableAlias(Bar), 'b')



class CompilerTest(TestCase):


    def test_class(self):
        """
        You can define functions to compile classes.
        """
        compiler = Compiler()

        class Foo(object):
            pass

        @compiler.when(Foo)
        def func(x, state):
            return (x, state, 'hey')


        foo = Foo()
        state = State()
        r = compiler.compile(foo, state)
        self.assertEqual(r, (foo, state, 'hey'),
                         "Should have used the class compiler")


    def test_type(self):
        """
        You can define functions to compile types
        """
        compiler = Compiler()

        @compiler.when(list)
        def foo(x, state):
            return x + ['hey']

        self.assertEqual(compiler.compile(['10']), ['10', 'hey'])


    def test_shared(self):
        """
        You can specify a list of things to that are compiled by the same
        function
        """
        compiler = Compiler()

        @compiler.when(list, str, dict)
        def foo(x, state):
            return 'NaN'

        self.assertEqual(compiler.compile('hey'), 'NaN')
        self.assertEqual(compiler.compile({}), 'NaN')
        self.assertEqual(compiler.compile([]), 'NaN')


    def test_subclass(self):
        """
        Subclasses will use the parent compiler unless another has been defined
        """
        class Parent(object):
            pass

        class Child1(Parent):
            pass

        class Child2(Parent):
            pass

        compiler = Compiler()

        @compiler.when(Parent)
        def parent(x, state):
            return 'parent', state


        @compiler.when(Child2)
        def child2(x, state):
            return 'child2', state

        state = State()
        self.assertEqual(compiler.compile(Parent(), state), ('parent', state))
        self.assertEqual(compiler.compile(Child1(), state), ('parent', state))
        self.assertEqual(compiler.compile(Child2(), state), ('child2', state))


    def test_parentCompiler(self):
        """
        You can provide a fallback compiler
        """
        fallback = Compiler()
        compiler = Compiler([fallback])

        @compiler.when(str)
        def foo(x, state):
            return x + ' compiled', state


        @fallback.when(str)
        def bar(x, state):
            return x + ' fallback', state


        @fallback.when(list)
        def baz(x, state):
            return x + ['fallback'], state


        state = State()
        self.assertEqual(compiler.compile('something', state),
                         ('something compiled', state),
                         "Should use the immediate compiler")
        self.assertEqual(compiler.compile([], state),
                         (['fallback'], state),
                         "Should use the fallback compiler but make the "
                         "original compiler available to the compile function")
        self.assertRaises(CompileError, compiler.compile, {})



class compilerTest(TestCase):
    """
    I test the global, default compiler
    """


    def test_str(self):
        self.assertEqual(base_compiler.compile('a'), ('?', ('a',)))


    def test_unicode(self):
        self.assertEqual(base_compiler.compile(u'a'), ('?', (u'a',)))


    def test_int(self):
        self.assertEqual(base_compiler.compile(10), ('?', (10,)))


    def test_date(self):
        self.assertEqual(base_compiler.compile(date(2000, 1, 1)),
                         ('?', (date(2000, 1, 1),)))


    def test_datetime(self):
        self.assertEqual(base_compiler.compile(datetime(2001, 1, 1)),
                         ('?', (datetime(2001, 1, 1),)))


    def test_bool(self):
        self.assertEqual(base_compiler.compile(True), ('?', (True,)))


    def test_None(self):
        self.assertEqual(base_compiler.compile(None), ('NULL', ()))


    def test_Property(self):
        class Foo(object):
            __sql_table__ = 'hey'
            id = Property()

        self.assertEqual(base_compiler.compile(Foo.id), ('a.id', ()))


    def test_Comparison(self):
        c = Comparison('a', 'b')
        c.op = 'hey'
        self.assertEqual(base_compiler.compile(c), ('? hey ?', ('a', 'b')))


    def test_Eq_None(self):
        self.assertEqual(base_compiler.compile(Eq('hey', None)),
                         ('? IS NULL', ('hey',)))
        self.assertEqual(base_compiler.compile(Eq(None, 'hey')),
                         ('NULL IS ?', ('hey',)))
        self.assertEqual(base_compiler.compile(Eq(None, None)),
                         ('NULL IS NULL', ()))


    def test_Neq_None(self):
        self.assertEqual(base_compiler.compile(Neq('hey', None)),
                         ('? IS NOT NULL', ('hey',)))
        self.assertEqual(base_compiler.compile(Neq(None, 'hey')),
                         ('NULL IS NOT ?', ('hey',)))
        self.assertEqual(base_compiler.compile(Neq(None, None)),
                         ('NULL IS NOT NULL', ()))


    def test_Gt(self):
        self.assertEqual(base_compiler.compile(Gt(1, 2)),
                         ('? > ?', (1, 2)))

    def test_Gte(self):
        self.assertEqual(base_compiler.compile(Gte(1, 2)),
                         ('? >= ?', (1, 2)))

    def test_Lt(self):
        self.assertEqual(base_compiler.compile(Lt(1, 2)),
                         ('? < ?', (1, 2)))

    def test_Lte(self):
        self.assertEqual(base_compiler.compile(Lte(1, 2)),
                         ('? <= ?', (1, 2)))


    def test_And(self):
        self.assertEqual(base_compiler.compile(And('hey', 'ho', 'ha')),
                         ('(? AND ? AND ?)', ('hey', 'ho', 'ha')))


    def test_Or(self):
        self.assertEqual(base_compiler.compile(Or('hey', 'ho', 'ha')),
                         ('(? OR ? OR ?)', ('hey', 'ho', 'ha')))


    def test_And_Or(self):
        self.assertEqual(base_compiler.compile(And(1, Or(2, 3), Or(4, And(5, 6)))),
                         ('(? AND (? OR ?) AND (? OR (? AND ?)))', (1,2,3,4,5,6)))


    def test_Join(self):
        class Foo(object):
            __sql_table__ = 'something'
            id = Property()

        sql, args = base_compiler.compile(Join(Foo, Eq(Foo.id, 10)))
        self.assertEqual(sql, 'JOIN something AS a ON a.id = ?')
        self.assertEqual(args, (10,))


    def test_Table(self):
        class Foo(object):
            __sql_table__ = 'foo'

        sql, args = base_compiler.compile(Table(Foo))
        self.assertEqual(sql, 'foo AS a')



