from zope.interface import Interface



class ITranslator(Interface):
    """
    I translate operations into functions to be run by an L{IRunner}.
    """

    def translate(operation):
        """
        Given an operation, return something that an L{IRunner} can run.
        """



class IRunner(Interface):
    """
    I run translated operations.
    """

    def run(translated_operation):
        """
        Run the operation.
        """