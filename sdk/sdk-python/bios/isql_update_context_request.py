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
from typing import List, Union

from ._utils import check_type
from .models.isql_request_message import ISqlRequestMessage, ISqlRequestType


class ISqlUpdateContext(ISqlRequestMessage):
    """Bios ISqlUpdateContext request class.
    it can be used to update the context entry
    example:
        req = ISqlStatement().update("name").set(dict with attributes and values to set)
            .where(key as a list of attributes, currently with only 1 attribute).build()
        responses = session.execute(req)
    """

    def __init__(self, request):
        super().__init__(ISqlRequestType.UPDATE_CONTEXT)
        self.values = request.values
        self.key = request.key
        self.name = request.name

    def __repr__(self):
        json_str = {
            "contentRepresentation": "CSV",
            "attributes": self.values,
            "primaryKey": self.key,
        }
        return json.dumps(json_str)


class ISqlUpdateContextRequestWhere:
    """Bios ISqlUpdateContextRequestWhere class"""

    def __init__(self, request):
        self.name = request.name
        self.values = request.values
        self.key = request.key

    def build(self) -> ISqlUpdateContext:
        """Builds the ISqlUpdateContext message"""
        return ISqlUpdateContext(self)


class ISqlUpdateContextRequestSet:
    """Bios ISqlUpdateContextRequestSet class"""

    def __init__(self, name, values):
        self.key = None
        self.values = values
        self.name = name

    def where(
        self, key: List[Union[str, int, float, bool, bytes]]
    ) -> ISqlUpdateContextRequestWhere:
        """Sets the key to be updated"""
        check_type(key, "key", list)
        self.key = key
        return ISqlUpdateContextRequestWhere(self)


class ISqlUpdateContextRequest:
    """Bios ISqlUpdateContextRequest class"""

    def __init__(self, name):
        self.name = name

    def set(self, key_value_dict: dict) -> ISqlUpdateContextRequestSet:
        """Sets the values to be updated; keys are attribute names, values are the new values."""
        check_type(key_value_dict, "key_value_dict", dict)
        values = []
        for k, v in key_value_dict.items():
            values.append({"name": k, "value": v})
        return ISqlUpdateContextRequestSet(self.name, values)

    def values(self, values: List[dict]) -> ISqlUpdateContextRequestSet:
        """Deprecated! Use set() instead.
        Sets the values to be updated; each dict has "name": attribute name, "value": new value."""
        check_type(values, "values", list)
        return ISqlUpdateContextRequestSet(self.name, values)
