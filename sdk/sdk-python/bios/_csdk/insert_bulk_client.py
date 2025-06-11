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

import json
import logging
import uuid

from .._proto._bios import data_pb2
from ..errors import ErrorCode, InsertBulkError, ServiceError
from ..models import InsertResponseRecord


class _ErrorHandler:
    def __init__(self):
        self._code_to_error = {}
        for error in ErrorCode:
            self._code_to_error[error.value] = error

    def resolve_code(self, status_code):
        error = self._code_to_error.get(status_code)
        if not error:
            raise ServiceError(
                ErrorCode.GENERIC_CLIENT_ERROR,
                "Unknown status code " + hex(status_code),
            )
        return error

    def handle_response(self, status_code, response, endpoint=None):
        error_code = self.resolve_code(status_code)
        if not error_code or error_code == ErrorCode.OK:
            return
        message = json.loads(response).get("message") if response else ""
        raise ServiceError(error_code, message, endpoint=endpoint)

    def handle_bulk_partial_response(self, status_code, response, endpoint=None):
        error_code = self.resolve_code(status_code)
        if not error_code or error_code == ErrorCode.OK:
            return None
        response = json.loads(response)
        results_with_error = response.get("resultsWithError")
        message = response.get("message")
        if results_with_error and len(results_with_error) > 0:
            return _InsertBulkBatchError(
                error_code,
                message,
                results_with_error,
                endpoint=endpoint,
            )

        return ServiceError(error_code, message, endpoint=endpoint)

    @staticmethod
    def to_service_error(error_code, error_message, endpoint=None):
        return ServiceError(error_code, error_message, endpoint=endpoint)


class _InsertBulkBatchError(ServiceError):
    _error_handler = _ErrorHandler()

    def __init__(
        self,
        error_code,
        message,
        results_with_error,
        params=None,
        endpoint=None,
    ):
        """The most basic constructor
        Args:
            error_code (ErrorCode): Service error code
            message (str): Error message
            results_with_error (dict): a result array that either contains IngestResponse
                            for each successful request in the bulk and error code for partial
                            failures.
            params (dict): Context parameters
            endpoint (str): Server endpoint where the error occurred
        """
        super().__init__(error_code, message, params, endpoint)
        self._results_with_error = results_with_error

    def __str__(self):
        return self.__repr__()

    def fill_responses(self, from_index, _responses):
        current_index = from_index
        first_time = True
        for result in self._results_with_error:
            error_code = self._error_handler.resolve_code(result["statusCode"])
            if not error_code or error_code == ErrorCode.OK:
                _responses[current_index] = {
                    "eventId": result["eventId"],
                    "timestamp": result["timestamp"],
                }
            else:
                message = result["errorMessage"]
                _responses[current_index] = self._error_handler.to_service_error(
                    error_code, message, endpoint=self.endpoint
                )
                if first_time:
                    first_time = False
                    self.error_code = error_code
                    self.message = message
                else:
                    if error_code != self.error_code:
                        self.error_code = ErrorCode.BULK_INGEST_FAILED
                        self.message = "Bulk Ingest Failed Partially with multiple failures"
            current_index = current_index + 1


class InsertBulkClient:
    """Insert bulk for protobuf"""

    _logger = logging.getLogger(__name__)
    _error_handler = _ErrorHandler()
    # this error reply is just a place holder in case there is an unexpected internal error
    _default_error = (
        '{"errorCode" :'
        + str(ErrorCode.GENERIC_CLIENT_ERROR.value)
        + ', "status" :400, "message" : "Unexpected Internal Error"}'
    )

    def __init__(self, bulk_request, num_events):
        self._bulk_request = bulk_request
        self._num_events = num_events
        self._remaining = num_events
        self._responses = [None] * num_events
        self._overall_error_code = None
        self._overall_error_message = None

    def make_bulk_request(self, from_index, to_index):
        req = data_pb2.InsertBulkRequest()
        req.signal = self._bulk_request.signal
        req.content_rep = self._bulk_request.content_representation
        if to_index > self._num_events:
            return None

        for i in range(from_index, to_index):
            record = data_pb2.Record()

            record.event_id = uuid.uuid1(node=uuid.getnode()).bytes
            record.string_values.append(self._bulk_request.values[i])
            req.record.append(record)

        data_out = req.SerializeToString()
        return data_out

    def process_partial_response(self, status_code, reply, endpoint, from_index, to_index):
        error = self._error_handler.handle_bulk_partial_response(status_code, reply, endpoint)
        if error is not None:
            if isinstance(error, _InsertBulkBatchError):
                error.fill_responses(from_index, self._responses)
            else:
                for i in range(from_index, to_index):
                    self._responses[i] = error
            if self._overall_error_code is None:
                self._overall_error_code = error.error_code
                self._overall_error_message = error.message
            if self._overall_error_code != error.error_code:
                self._overall_error_code = ErrorCode.BULK_INGEST_FAILED
                self._overall_error_message = "Bulk Insert Failed Partially with multiple failures"
        else:
            resp_protos = data_pb2.InsertBulkSuccessResponse()
            resp_protos.ParseFromString(reply)
            idx = from_index
            for result in resp_protos.responses:
                if idx >= to_index:
                    self._overall_error_code = ErrorCode.GENERIC_CLIENT_ERROR
                    self._overall_error_message = (
                        "Internal Error: Bulk Insert response has more responses than expected"
                    )
                    break
                self._responses[idx] = InsertResponseRecord(result)
                idx = idx + 1

            if idx < to_index:
                self._overall_error_code = ErrorCode.GENERIC_CLIENT_ERROR
                self._overall_error_message = (
                    "Internal Error: Bulk Insert partial response is missing responses"
                )
                for i in range(idx, to_index):
                    self._responses[i] = self._error_handler.to_service_error(
                        self._overall_error_code,
                        self._overall_error_message,
                    )

        self._logger.debug(
            "From=%s, Current=%s, num_events=%s",
            from_index,
            to_index,
            self._num_events,
        )
        self._remaining -= to_index - from_index
        if self._remaining == 0:
            # we are done. Indicate end of insert bulk
            if self._overall_error_code:
                return (
                    self._overall_error_code.value,
                    self._default_error,
                )
            return 0, None

        return None, None

    def process_error_response(self, err):
        if self._overall_error_code:
            # there is an error
            success = {}
            errors = {}
            num_responses = len(self._responses)
            for i in range(num_responses):
                response = self._responses[i]
                if isinstance(response, ServiceError):
                    errors[i] = response
                else:
                    success[i] = response
            raise InsertBulkError(
                self._overall_error_code,
                self._overall_error_message,
                success,
                errors,
            )
        raise err

    def process_response(self):
        if self._overall_error_code:
            # this is unexpected
            raise ServiceError(
                ErrorCode.GENERIC_CLIENT_ERROR,
                "Unexpected failure due to an Internal error",
            )
        return self._responses
