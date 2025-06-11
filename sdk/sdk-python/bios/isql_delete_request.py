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
import warnings
from typing import List

from bios._utils import check_type

from .models.isql_request_message import ISqlRequestMessage, ISqlRequestType


class ISqlDeleteContext(ISqlRequestMessage):
    """Bios class to create a delete context isql message
    it is built using ISqlRequest.
    to execute the delete request, use session.execute
    """

    def __init__(self, request):
        super().__init__(ISqlRequestType.DELETE_CONTEXT)
        self.keys = request.keys
        self.name = request.name

    def __repr__(self):
        json_dict = {
            "contentRepresentation": "UNTYPED",
            "primaryKeys": self.keys,
        }
        return json.dumps(json_dict)


class ISqlDeleteFromContextWhere:
    """Bios ISqlDeleteFromContextWhere class"""

    def __init__(self, request):
        self.name = request.name
        self.keys = request.keys

    def build(self) -> ISqlDeleteContext:
        """Creates ISqlDeleteContext message to delete a context"""
        return ISqlDeleteContext(self)


class ISqlDeleteFromContext:
    """Bios ISqlDeleteFromContext class"""

    def __init__(self, name):
        check_type(name, "name", str)
        self.name = name
        self.keys = None

    def where(self, keys: List[List[object]]) -> ISqlDeleteFromContextWhere:
        """Sets keys to be deleted"""
        check_type(keys, "keys", list)
        self.keys = keys
        return ISqlDeleteFromContextWhere(self)


class ISqlDeleteContextRequest:
    """Bios ISqlDeleteContextRequest class"""

    def from_context(self, name: str) -> ISqlDeleteFromContext:
        """Sets the name of the context for the delete request"""
        check_type(name, "name", str)
        return ISqlDeleteFromContext(name)

    def fromContext(self, name: str) -> ISqlDeleteFromContext:  # pylint: disable=invalid-name
        """DEPRECATED: Sets the name of the context for the delete request"""
        warnings.warn("Use from_context instead", DeprecationWarning)
        return self.from_context(name)
