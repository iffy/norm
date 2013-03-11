from zope.interface import Interface, Attribute



class IOperation(Interface):
    """
    An atomic operation
    """

    op_name = Attribute("""A unique name for this operation""")



class ITranslator(Interface):
    """
    I translate operations into functions to be run by an L{IRunner}.
    """

    def translate(operation):
        """
        Given an operation, return something that an L{IRunner} can run.
        """


    def translateParams(sql):
        """
        Convert an SQL statement using ? to an SQL statement more appropriate
        for the database being translated to.
        """



class IRunner(Interface):
    """
    I run translated operations.
    """

    def run(translated_operation):
        """
        Run the operation.
        """