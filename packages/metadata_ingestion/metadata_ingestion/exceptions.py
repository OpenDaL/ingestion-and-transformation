"""
Custom Exceptions Module

Contains the exceptions used to further clarify and handle frequently occuring
errors

Copyright (C) 2021  Tom Brouwer

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
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

    def __init__(self, status_code: int, message: str):
        """
        Initializes the NonRetryableHTTPStatus instance

        Args:
            status_code:
                The Status code returned by the server
            message:
                The message describing the meaning of the returned status
        """
        message = 'Server returned status {}, {}'.format(status_code, message)
        super().__init__(message)


class InvalidStatusCode(IngestionException):
    """
    Raised when an invalid status code is returned
    """

    def __init__(self, status_code: int, text_response: str = None):
        """
        Initializes the InvalidStatusCode instance

        Args:
            status_code:
                The status code returned by the server
            text_response:
                The text of the response returned by the server
        """
        message = 'Server returned invalid status {}'.format(status_code)
        self.status_code = status_code
        if text_response is not None:
            message += ', with text response {}'.format(text_response)
        super().__init__(message)
