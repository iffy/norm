"""
Schema patches/migrations
"""

from twisted.internet import defer
from norm.operation import SQL, Insert



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
        @param func: A function to be called with a runner
            as the only argument.

        @rtype: int
        @return: The patch number added, starting at 1.
        """
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
            sem.run(func, runner)
            # record the application
            sem.run(self._recordPatch, runner, i, name)
        return applied

    def _recordPatch(self, runner, number, name):
        return runner.run(Insert(self.patch_table_name, [
            ('number', number),
            ('name', name),
        ]))


    def _appliedPatches(self, runner):
        d = runner.run(SQL('select number from ' + self.patch_table_name))
        d.addErrback(lambda x: self._createPatchTable(runner))
        d.addCallback(lambda x: [x[0] for x in x])
        return d


    def _createPatchTable(self, runner):
        d = runner.run(SQL('''create table ''' + self.patch_table_name + '''(
                            number integer,
                            name text,
                            created timestamp default current_timestamp
                            )'''))
        return d.addCallback(lambda x: [])



class SQLPatch(object):


    def __init__(self, *sqls):
        self.sqls = sqls


    def __call__(self, runner):
        sem = defer.DeferredSemaphore(1)
        dlist = []
        for sql in self.sqls:
            d = sem.run(runner.run, SQL(sql))
            dlist.append(d)
        return defer.gatherResults(dlist)


