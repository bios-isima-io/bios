#
# Copyright (C) 2025 Isima, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from enum import Enum, unique


@unique
class ErrorCode(Enum):
    """Enum class that defines TFOS error codes."""

    OK = 0
    """OK"""

    ##########################
    # 0x1nnnnn Error from client

    GENERIC_CLIENT_ERROR = 0x100001
    """An unexpected problem happened on client side."""
    INVALID_ARGUMENT = 0x100002
    """Invalid argument was given to an SDK method."""
    SESSION_INACTIVE = 0x100003
    """ A request for an operation was issued to the client,
       but it is closed already."""
    PARSER_ERROR = 0x100004
    """Failed to convert a string to a TFOS request object."""
    CLIENT_ALREADY_STARTED = 0x100005
    """Tried to start already started client"""
    REQUEST_TOO_LARGE = 0x100006
    """Request entity too large.

       The request size is limited at 1 million bytes.
    """
    INVALID_STATE = 0x100007
    """Client is in invalid state"""
    CLIENT_CHANNEL_ERROR = 0x100008
    """SDK failed to communicate with peer for an unexpected reason"""

    ##########################
    # 0x2nnnnn Error from server

    # 0x20nnnn Service level errors
    SERVER_CONNECTION_FAILURE = 0x200001
    """ SDK failed to connect server. """
    SERVER_CHANNEL_ERROR = 0x200002
    """ SDK encountered a communication error. """
    UNAUTHORIZED = 0x200003
    """ Not authorized. """
    FORBIDDEN = 0x200004
    """ Permission denied. """
    SERVICE_UNAVAILABLE = 0x200005
    """ Server is currently unavailable. """
    GENERIC_SERVER_ERROR = 0x200006
    """ An unexpected problem happened on the server side. """
    TIMEOUT = 0x200007
    """ Operation has timed out. """
    BAD_GATEWAY = 0x200008
    """ Load balancer proxy failed due to upstream server crash """
    SERVICE_UNDEPLOYED = 0x200009
    """ Requesteded resource was not found on the server """
    OPERATION_CANCELLED = 0x20000A
    """ Operation has been cancelled by peer """
    OPERATION_UNEXECUTABLE = 0x20000B
    """ Unable to execute the requested operation """
    SESSION_EXPIRED = 0x20000C
    """ Session has expired """

    # 0x21nnnn Semantic error
    NO_SUCH_TENANT = 0x210001
    """ SDK tried an operation that requires a tenant, but the specified
        tenant does not exist. """
    NO_SUCH_STREAM = 0x210002
    """ SDK tried an operation that requires a stream, but the specified
        stream does not exist. """
    NOT_FOUND = 0x210003
    """ Target entity does not exist in server """
    TENANT_ALREADY_EXISTS = 0x210004
    """ SDK tried to add an tenant, but specified tenant already exists. """
    STREAM_ALREADY_EXISTS = 0x210005
    """ SDK tried to add a stream, but specified stream already exists. """
    RESOURCE_ALREADY_EXISTS = 0x210006
    """ SDK tried to add an entity to server, but the target already exists. """
    BAD_INPUT = 0x210007
    """ Server rejected input due to invalid data or format. """
    NOT_IMPLEMENTED = 0x210008
    """ Not implemented """
    CONSTRAINT_WARNING = 0x210009
    """ Constraint warning. Set force option to override. """
    SCHEMA_VERSION_CHANGED = 0x21000B
    """Given stream version no longer exists"""
    INVALID_REQUEST = 0x21000C
    """Server rejected request due to invalid request"""
    BULK_INGEST_FAILED = 0x21000D
    """Bulk ingest failed"""
    SERVER_DATA_ERROR = 0x21000E
    """Server returned malformed data"""
    SCHEMA_MISMATCHED = 0x21000F
    """Schema mismatched"""
    USER_ID_NOT_VERIFIED = 0x210010
    """User ID is not verified yet"""

    def __repr__(self):
        return f"<{self.__class__.__name__!s}.{self.name!s}: {self.value:#x}>"


class ServiceError(Exception):
    """Exception raised when an SDK error has occurred.

    A ServiceError object has following properties:

    * error_code -- (:class:`ErrorCode`) Enum that indicates the type of error
    * message -- (str) Error message
    * endpoint -- (str) Server endpoint where the error occurred. The value may be None.
    * params -- (dict) Context parameters
    """

    def __init__(self, error_code: ErrorCode, message, params=None, endpoint=None):
        """The most basic constructor
        Attributes:
            error_code (ErrorCode): Service error code
            message (str): Error message
            params (dict): Context parameters
            endpoint (str): Server endpoint where the error occurred
        """
        super().__init__()
        self.error_code = error_code
        self.message = message
        self.params = params or {}
        self.endpoint = endpoint

    def __repr__(self):
        out = f"error_code={self.error_code.name},"
        if self.endpoint:
            out += f" endpoint={self.endpoint},"
        out += f" message={self.message}"
        if self.params:
            out += f", params={self.params!s}"
        return out

    def __str__(self):
        return self.__repr__()


class InsertBulkError(ServiceError):
    """Exception raised when an Insert bulk fails

    A InsertBulkError object has following additional properties:

    * responses -- (dict) for every successful response, with index as the key
    * errors -- (dict) for every failed insert, with index of the corresponding insert request
                          as key.
    """

    def __init__(
        self,
        error_code,
        message,
        responses,
        errors,
        params=None,
        endpoint=None,
    ):
        """The most basic constructor
        Args:
            error_code (ErrorCode): Service error code
            message (str): Error message
            responses (dict): a result array that either contains InsertResponse
                            for each successful request in the bulk, with request index as key
            errors (dict): a result array that either contains ServiceError
                            for each unsuccessful request in the bulk, with request index as key
            params (dict): Context parameters
            endpoint (str): Server endpoint where the error occurred
        """
        super().__init__(error_code, message, params, endpoint)
        self.responses = responses
        self.errors = errors

    def get_errors(self):
        return self.errors

    def __str__(self):
        return self.__repr__()
