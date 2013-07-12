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
        self._patchnames = set()


    def add(self, name, func):
        """
        Add a patch function.

        @param name: Name describing the patch.
        @param func: A function to be called with an asynchronous cursor
            as the only argument.  A string, list or tuple of strings may also
            be provided, in which case C{func} will be wrapped in L{SQLPatch}.

        @raise ValueError: If a patch name is reused.
        """
        if name in self._patchnames:
            raise ValueError('There is already a patch named %r' % (name,))
        if type(func) in (str, unicode):
            func = SQLPatch(func)
        elif type(func) in (tuple, list):
            func = SQLPatch(*func)
        self.patches.append((name, func))
        self._patchnames.add(name)


    def upgrade(self, runner, stop_at_patch=None):
        """
        Upgrade a database through the given runner.

        @param runner: An L{IRunner}.
        @param stop_at_patch: The name of the patch to stop at.  This will
            correspond to the name supplied to L{add}.

        @return: A list of the patches applied
        """
        already = self._appliedPatches(runner)
        return already.addCallback(self._applyMissing, runner, stop_at_patch)
    

    def  _applyMissing(self, already, runner, stop_at_patch=None):
        applied = []
        sem = defer.DeferredSemaphore(1)
        stop = False
        for name,func in self.patches:
            if stop:
                break
            if stop_at_patch is not None and stop_at_patch == name:
                # stopping after this one
                stop = True
            if name in already:
                # patch already applied
                continue
            applied.append(name)
            # apply the patch
            sem.run(runner.runInteraction, func)
            # record the application
            sem.run(runner.runInteraction, self._recordPatch, name)
        return applied


    def _recordPatch(self, cursor, name):
        return cursor.execute('insert into %s (name) values (?)' % (
                              self.patch_table_name,), (name,))


    def _appliedPatches(self, runner):
        d = runner.runQuery('select name from ' + self.patch_table_name)
        d.addErrback(lambda x: self._createPatchTable(runner))
        d.addCallback(lambda a: [x[0] for x in a])
        return d


    def _createPatchTable(self, runner):
        d = runner.runOperation(
            '''create table ''' + self.patch_table_name + '''(
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


