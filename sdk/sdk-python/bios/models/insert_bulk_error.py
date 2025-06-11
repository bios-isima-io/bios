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

from ..errors import ServiceError


# pylint: disable=duplicate-code
class IngestBulkError(ServiceError):
    """Exception raised when an Ingest bulk fails

    A IngestBulkError object has following additional properties:

    * responses -- (dict) for every successful response, with index as the key
    * errors -- (dict) for every failed ingest, with index of the corresponding ingest request
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
            responses (dict): a result array that either contains IngestResponse
                            for each successful request in the bulk, with request index as key
            errors (dict): a result array that either contains ServiceError
                            for each unsuccessful request in the bulk, with request index as key
            params (dict): Context parameters
            endpoint (str): Server endpoint where the error occurred
        """
        super().__init__(error_code, message, params, endpoint)
        self.responses = responses
        self.errors = errors

    def __str__(self):
        return self.__repr__()
