from twisted.internet import defer
from zope.interface.verify import verifyObject


from norm.interface import ITranslator, IRunner
from norm.operation import Insert, SQL



class AllInOneRunner(object):


    def __init__(self, translator, runner):
        self.translator = translator
        self.runner = runner


    @defer.inlineCallbacks
    def run(self, op):
        translated = self.translator.translate(op)
        result = yield self.runner.run(translated)
        defer.returnValue(result)



class TranslateRunnerTestMixin(object):
    """
    I functionally test a Translator/Runner pair.
    """


    def getRunner(self, translator):
        raise NotImplementedError("Provide an IRunner with a connection to a "
                                  "database with the default tables in it")


    def getTranslator(self):
        raise NotImplementedError("Provide an ITranslator for the given "
                                  "IRunner")


    @defer.inlineCallbacks
    def getExecutor(self):
        translator = yield self.getTranslator()
        runner = yield self.getRunner(translator)
        defer.returnValue(runner)


    @defer.inlineCallbacks
    def test_IRunner(self):
        runner = yield self.getRunner(None)
        verifyObject(IRunner, runner)


    @defer.inlineCallbacks
    def test_ITranslator(self):
        translator = yield self.getTranslator()
        verifyObject(ITranslator, translator)


    @defer.inlineCallbacks
    def r(self, runner, translator, op):
        translated = translator.translate(op)
        value = yield runner.run(translated)
        defer.returnValue(value)


    @defer.inlineCallbacks
    def test_Insert(self):
        """
        You can Insert a record and get the last insert row id out.
        """
        e = yield self.getExecutor()        
        result = yield e.run(Insert('foo', lastrowid=True))
        self.assertEqual(result, 1, "Should return the primary key")


    @defer.inlineCallbacks
    def test_Insert_data(self):
        """
        You can insert a record with some data.
        """
        e = yield self.getExecutor()

        result = yield e.run(Insert('foo', [('name', 'something')], lastrowid=True))
        self.assertEqual(result, 1, "Should return the primary key")

        rows = yield e.run(SQL('select id, name from foo'))
        self.assertEqual(rows, [
            (1, 'something'),
        ])


    @defer.inlineCallbacks
    def test_Insert_lastrowidOptional(self):
        """
        You can have the last row id not be returned
        """
        e = yield self.getExecutor()

        result = yield e.run(Insert('foo', [('name', 'something')], lastrowid=False))
        self.assertEqual(result, None, "Should not return the last row id")


    @defer.inlineCallbacks
    def test_SQL(self):
        """
        You can do a raw SQL statement.
        """
        e = yield self.getExecutor()

        # give it some data
        yield e.run(Insert('foo', [('name', 'something')]))
        yield e.run(Insert('foo', [('name', 'another')]))

        # select the data
        rows = yield e.run(SQL('select name from foo where name = ?', ('another',)))
        self.assertEqual(rows, [
            ('another',)
        ])


    @defer.inlineCallbacks
    def test_SQL_noResults(self):
        """
        You can do a raw SQL statement that doesn't return rows
        """
        e = yield self.getExecutor()

        yield e.run(SQL('create table a (id integer)'))
        self.addCleanup(e.run, SQL('drop table a'))


