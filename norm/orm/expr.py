# Copyright (c) Matt Haggard.
# See LICENSE for details.

from norm.orm.base import classInfo, Property

from collections import defaultdict
from itertools import product
from datetime import date, datetime


class CompileError(Exception):
    pass



class Query(object):
    """
    XXX
    """


    def __init__(self, select, constraints=None):
        if type(select) not in (list, tuple):
            select = (select,)
        self.select = select
        self.constraints = constraints
        self._classes = []
        self._props = []
        self._process()


    def _process(self):
        self._classes = []
        self._props = []
        for item in self.select:
            info = classInfo(item)
            keys = sorted(info.attributes.values())
            self._props.extend(keys)
            self._classes.append(item)


    def properties(self):
        """
        Get a tuple of the Properties that will be returned by the query.
        """
        return tuple(self._props)


    def classes(self):
        return self._classes


    def find(self, select, constraints):
        """
        XXX
        """
        return Query(select, And(self.constraints, constraints))



def aliases(pool='abcdefghijklmnopqrstuvwxyz'):
    for i in xrange(1, 255):
        for item in product(pool, repeat=i):
            yield ''.join(item)


class State(object):
    """
    Compilation state and life-line to the whole compilation process.
    """

    compiler = None


    def __init__(self):
        pool = aliases()
        self._aliases = defaultdict(lambda:pool.next())
        self.classes = []


    def compile(self, thing):
        return self.compiler.compile(thing, self)


    def tableAlias(self, cls):
        """
        Return a name that can be used (repeatedly) as an alias for a class'
        table.
        """
        alias = self._aliases[cls]
        if cls not in self.classes:
            self.classes.append(cls)
        return alias



class Compiler(object):
    """
    I compile "things" into "other things" (most typically, objects into SQL)
    """


    def __init__(self, fallbacks=None):
        self.classes = {}
        self.fallbacks = fallbacks or []


    def when(self, *cls):
        def deco(f):
            for c in cls:
                self.classes[c] = f
            return f
        return deco


    def compile(self, thing, state=None):
        state = state or State()
        if not state.compiler:
            state.compiler = self
        cls = thing.__class__
        classes = [cls] + list(cls.__bases__)
        for c in classes:
            if c in self.classes:
                return self.classes[c](thing, state)
        for fallback in self.fallbacks:
            try:
                return fallback.compile(thing, state)
            except CompileError:
                pass
        raise CompileError("I don't know how to compile %r" % (thing,))


compiler = Compiler()


@compiler.when(Query)
def compile_Query(query, state):
    # select
    props = query.properties()
    columns = []
    select_args = []
    for prop in props:
        s, q = state.compile(prop)
        columns.append(s)
        select_args.extend(q)
    select_clause = ['SELECT %s' % (','.join(columns),)]

    # where
    where_clause = []
    where_args = []
    constraints = query.constraints
    if constraints:
        s, a = state.compile(constraints)
        where_clause = ['WHERE %s' % (s,)]
        where_args.extend(a)

    # table
    classes = [x for x in state.classes]
    from_args = []
    tables = []
    for cls in classes:
        s, a = state.compile(Table(cls))
        tables.append(s)
        from_args.extend(a)

    from_clause = ['FROM %s' % (','.join(tables))]
    

    sql = ' '.join(select_clause + from_clause + where_clause)
    args = tuple(select_args + from_args + where_args)
    return sql, args



class Table(object):
    """
    XXX
    """

    def __init__(self, cls):
        self.cls = cls


@compiler.when(Table)
def compile_Table(table, state):
    info = classInfo(table.cls)
    return '%s AS %s' % (info.table, state.tableAlias(table.cls)), ()


@compiler.when(Property)
def compile_Property(x, state):
    alias = state.tableAlias(x.cls)
    return '%s.%s' % (alias, x.column_name), ()


@compiler.when(str, unicode, int, bool, date, datetime)
def compile_str(x, state):
    return ('?', (x,))


@compiler.when(type(None))
def compile_None(x, state):
    return ('NULL', ())



class Comparison(object):

    op = None

    def __init__(self, left, right):
        self.left = left
        self.right = right


class Eq(Comparison):
    op = '='


class Neq(Comparison):
    op = '!='


@compiler.when(Comparison)
def compile_Comparison(x, state):
    left, left_args = state.compile(x.left)
    right, right_args = state.compile(x.right)
    return ('%s %s %s' % (left, x.op, right), left_args + right_args)


def compile_Comparison_null(x, null_op, state):
    op = x.op
    left = ''
    left_args = ()
    right = ''
    right_args = ()
    if x.left is None:
        left = 'NULL'
        op = null_op
    else:
        left, left_args = state.compile(x.left)
    if x.right is None:
        right = 'NULL'
        op = null_op
    else:
        right, right_args = state.compile(x.right)
    return ('%s %s %s' % (left, op, right), left_args + right_args)


@compiler.when(Eq)
def compile_Eq(x, state):
    return compile_Comparison_null(x, 'IS', state)


@compiler.when(Neq)
def compile_Neq(x, state):
    return compile_Comparison_null(x, 'IS NOT', state)



class LogicalBinaryOp(object):

    join = None

    def __init__(self, *items):
        self.items = items

class And(LogicalBinaryOp):
    join = ' AND '


class Or(LogicalBinaryOp):
    join = ' OR '


@compiler.when(LogicalBinaryOp)
def compile_Joiner(x, state):
    parts = []
    args = ()
    for item in x.items:
        sql, item_args = state.compile(item)
        parts.append(sql)
        args = args + item_args

    return ('('+x.join.join(parts)+')', args)


class Join(object):

    def __init__(self, cls, on):
        self.cls = cls
        self.on = on


@compiler.when(Join)
def compile_Join(x, state):
    table = classInfo(x.cls).table
    alias = state.tableAlias(x.cls)
    on_sql, on_args = state.compile(x.on)
    return ('JOIN %s AS %s ON %s' % (table, alias, on_sql), on_args)





