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

from bios.errors import ErrorCode, ServiceError


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

    @staticmethod
    def to_service_error(error_code, error_message, endpoint=None):
        return ServiceError(error_code, error_message, endpoint=endpoint)
