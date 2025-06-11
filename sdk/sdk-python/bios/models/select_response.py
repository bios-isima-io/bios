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
from collections import OrderedDict
from enum import Enum
from typing import List

from .isql_response_message import ISqlResponse, ISqlResponseType


class AttributeType(Enum):
    """Bios AttributeType class"""

    INTEGER = 1
    DECIMAL = 2
    STRING = 3
    BLOB = 4
    BOOLEAN = 5

    def __repr__(self):
        return self.name


class AttributeDefinition:
    """Bios AttributeDefinition class"""

    def __init__(self, name, atype):
        self.name = name
        self.atype = atype

    def get_name(self):
        return self.name

    def get_type(self):
        return self.atype

    def __repr__(self):
        obj = self.__dict__.copy()
        obj["type"] = self.atype
        del obj["atype"]
        return str(obj)


class AttributeDefinitions:
    """Bios AttributeDefinitions class"""

    def __init__(self):
        self.definitions = []
        self.names = {}

    def append(self, definition):
        self.names[definition.name] = len(self.definitions)
        self.definitions.append(definition)

    def get(self, index):
        return self.definitions[index]

    def length(self):
        return len(self.definitions)

    def __repr__(self):
        return str(self.definitions)


class Record:
    """Bios Record class"""

    def __init__(self, attributes, definitions, event_id=None, timestamp=None, primary_key=None):
        self.attributes = attributes
        self.definitions = definitions
        self.event_id = event_id
        self.timestamp = timestamp
        if primary_key:
            self.primary_key = [self.get(key) for key in primary_key]
        else:
            self.primary_key = None

    def get_event_id(self):
        return self.event_id

    def get_timestamp(self):
        return self.timestamp

    def get_primary_key(self):
        return self.primary_key

    def get(self, name):
        """Gets an attribute value

        Args:
            name (str): Attribute name

        Returns:
            Attribute data
        """
        index = self.definitions.names.get(name)
        return self.attributes[index] if index is not None else None

    def get_by_index(self, index):
        return self.attributes[index]

    def get_type(self, name):
        index = self.definitions.names.get(name)
        return self.definitions.definitions[index].atype if index is not None else None

    def to_dict(self, include_metadata: bool = False):
        """Converts to dict"""
        out = {name: self.get(name) for name in self.definitions.names}
        if include_metadata:
            out["eventId"] = self.event_id
            out["ingestTimestamp"] = self.timestamp
        return out

    def __iter__(self):
        if self.event_id:
            yield ("event_id", self.event_id)
        if self.timestamp:
            yield ("timestamp", self.timestamp)
        if self.primary_key:
            yield ("primary_key", self.primary_key)
        yield (
            "attributes",
            OrderedDict([(name, self.get(name)) for name in self.definitions.names]),
        )

    def __repr__(self):
        return json.dumps(OrderedDict((name, self.get(name)) for name in self.definitions.names))


class DataWindow:
    """Data window object
    properties:
      window_begin_time (int): Timestamp at the beginning of the window
      records (List[Record]): Records
    """

    def __init__(self, window_begin_time, records):
        self.window_begin_time = window_begin_time
        self.records = records

    def __repr__(self):
        out = [f"timestamp: {self.window_begin_time}"]
        for record in self.records:
            out.append(f"  {record}")
        return "\n".join(out)

    def get_timestamp(self):
        """Returns the timestamp at the beginning of the time window as milliseconds since epoch"""
        return self.window_begin_time

    def get_records(self):
        return self.records

    def to_dict(self):
        return {
            "timestamp": self.window_begin_time,
            "records": [record.to_dict() for record in self.records],
        }


class SelectResponse(ISqlResponse):
    """Bios SelectResponse class"""

    def __init__(self, definitions, data_windows):
        super().__init__(ISqlResponseType.SELECT)
        self.definitions = definitions
        self.data_windows = data_windows

    def get_definitions(self):
        return self.definitions

    def get_data_windows(self):
        return self.data_windows

    def __repr__(self):
        out = ["-----"]
        out.extend([str(data_window) for data_window in self.data_windows])
        return "\n".join(out)

    def _record_to_dict(self, record):
        ord_dict = OrderedDict()
        for key in self.definitions.names:
            ord_dict[key] = record.get(key)
        ord_dict["eventId"] = str(record.event_id)
        ord_dict["ingestTimestamp"] = record.timestamp
        return ord_dict

    def to_dict(self) -> List[OrderedDict]:
        """
        Converts the select response into a pandas dataframe compatible
        list of ordered dictionaries.

        Returns: List[OrderedDict]: Records
        """
        if not self.data_windows:
            return []

        if len(self.data_windows) == 1:
            window = self.data_windows[0]
            return [self._record_to_dict(record) for record in window.records]

        return [window.to_dict() for window in self.data_windows]


class SelectContextResponse(ISqlResponse):
    """Bios SelectContextResponse class"""

    def __init__(self, response):
        super().__init__(ISqlResponseType.SELECT_CONTEXT_EX)
        self._response = response

    def get_records(self):
        return self._response["entries"]

    def get_aggregator(self):
        return self._response["aggregator"]

    def to_dict(self) -> List[OrderedDict]:
        """
        Converts the select response into a pandas dataframe compatible
        list of ordered dictionary.

        Returns: List[OrderedDict]: records
        """
        return self.get_records()


class ContextRecords(ISqlResponse):
    """Bios ContextRecords class"""

    def __init__(self, response):
        super().__init__(ISqlResponseType.SELECT_CONTEXT)
        self._response = response
        self.primary_key = response.get("primaryKey")
        self.primary_keys = self.primary_key  # Deprecated
        self.definitions = AttributeDefinitions()
        for def_src in response["definitions"]:
            try:
                atype = AttributeType[def_src["type"].upper()]
            except KeyError:
                atype = None
            definition = AttributeDefinition(def_src["attributeName"], atype)
            self.definitions.append(definition)
        self.records = [
            Record(
                entry["attributes"],
                self.definitions,
                timestamp=entry.get("timestamp"),
                primary_key=self.primary_key,
            )
            for entry in response["entries"]
        ]

    def get_records(self):
        return self.records

    def get_primary_key(self):
        return self.primary_key

    def get_primary_keys(self):
        """Deprecated"""
        warnings.warn("Use get_primary_key instead", DeprecationWarning)
        return self.primary_key

    def _record_to_dict(self, record):
        ord_dict = OrderedDict()
        for pkey_component in self.primary_key:
            ord_dict[pkey_component] = record.get(pkey_component)
        for key in self.definitions.names:
            if key not in self.primary_key:
                ord_dict[key] = record.get(key)
        return ord_dict

    def to_dict(self) -> List[OrderedDict]:
        return [self._record_to_dict(record) for record in self.records]
