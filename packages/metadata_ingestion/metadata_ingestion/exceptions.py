"""
EXCEPTIONS SUB-MODULE

Defines the custom exceptions
"""


class IngestionException(Exception):
    """
    Base class for all exceptions
    """
    pass


class UnexpectedDataError(IngestionException):
    """
    Error for when unexpected data is returned
    """
    pass


class TooMuchDataError(IngestionException):
    """
    An API returned too much data, probable cause is duplicates or API errors
    """
    pass


class NoResultsError(IngestionException):
    """
    Raised when no data is harvested from an API
    """
    pass


class NonRetryableHTTPStatus(IngestionException):
    """
    Raised when a HTTP status is returned, for which no retries will be made
    """

    def __init__(self, status_code, code_meaning):
        message = 'Server returned status {}, {}'.format(status_code,
                                                         code_meaning)
        super().__init__(message)


class InvalidStatusCode(IngestionException):
    """
    Raised when an invalid status code is returned
    """

    def __init__(self, status_code, text_response=None):
        message = 'Server returned invalid status {}'.format(status_code)
        self.status_code = status_code
        if text_response is not None:
            message += ', with text response {}'.format(text_response)
        super().__init__(message)
