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

from enum import Enum


class ISqlRequestType(Enum):
    """Bios ISqlRequestType class"""

    # pylint: disable=duplicate-code
    INSERT = 0
    INSERT_BULK = 1
    SELECT = 2
    UPSERT = 3
    SELECT_CONTEXT = 4
    UPDATE_CONTEXT = 5
    DELETE_CONTEXT = 6
    SELECT_CONTEXT_EX = 7
    ATOMIC_MUTATION = 8


class ISqlRequestMessage:
    """Bios ISqlRequestMessage class"""

    def __init__(self, request_type):
        self._type = request_type

    def get_type(self):
        return self._type
