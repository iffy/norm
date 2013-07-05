# Copyright (c) Matt Haggard.
# See LICENSE for details.

"""
Schema patches/migrations
"""

from twisted.internet import defer



class Patcher(object):
    """
    I hold patch functions for a database.
    """

    def __init__(self, patch_table_name='_patch'):
        self.patch_table_name = patch_table_name
        self.patches = []


    def add(self, name, func):
        """
        Add a patch function.

        @param name: Name describing the patch.
        @param func: A function to be called with an asynchronous cursor
            as the only argument.  A string, list or tuple of strings may also
            be provided, in which case C{func} will be wrapped in L{SQLPatch}.

        @rtype: int
        @return: The patch number added, starting at 1.
        """
        if type(func) in (str, unicode):
            func = SQLPatch(func)
        elif type(func) in (tuple, list):
            func = SQLPatch(*func)
        self.patches.append((name,func))
        return len(self.patches)


    def upgrade(self, runner):
        """
        Upgrade a database through the given runner.

        @return: A list of the patches applied
        """
        already = self._appliedPatches(runner)
        return already.addCallback(self._applyMissing, runner)
    

    def  _applyMissing(self, already, runner):
        applied = []
        sem = defer.DeferredSemaphore(1)
        for i,(name,func) in enumerate(self.patches, 1):
            if i in already:
                # patch already applied
                continue
            applied.append((i, name))
            # apply the patch
            sem.run(runner.runInteraction, func)
            # record the application
            sem.run(runner.runInteraction, self._recordPatch, i, name)
        return applied


    def _recordPatch(self, cursor, number, name):
        return cursor.execute('insert into %s (number, name) values (?,?)' % (
                              self.patch_table_name,), (number, name))


    def _appliedPatches(self, runner):
        d = runner.runQuery('select number from ' + self.patch_table_name)
        d.addErrback(lambda x: self._createPatchTable(runner))
        d.addCallback(lambda x: [x[0] for x in x])
        return d


    def _createPatchTable(self, runner):
        d = runner.runOperation('''create table ''' + self.patch_table_name + '''(
                            number integer,
                            name text,
                            created timestamp default current_timestamp
                            )''')
        return d.addCallback(lambda x: [])



class SQLPatch(object):


    def __init__(self, *sqls):
        self.sqls = sqls


    def __call__(self, cursor):
        sem = defer.DeferredSemaphore(1)
        dlist = []
        for sql in self.sqls:
            d = sem.run(cursor.execute, sql)
            dlist.append(d)
        return defer.gatherResults(dlist)


