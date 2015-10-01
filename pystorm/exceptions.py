""" Pystorm-specific exceptions """

class StormWentAwayError(Exception):
    """Raised when the connection between the component and Storm terminates.
    """

    def __init__(self):
        message = "Got EOF while reading from Storm"
        super(StormWentAwayError, self).__init__(message)