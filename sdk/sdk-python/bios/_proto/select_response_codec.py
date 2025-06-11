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

import uuid

from .._proto._bios.data_pb2 import AttributeType as AttributeTypeProto
from .._proto._bios.data_pb2 import SelectResponse as SelectResponseProto
from ..models import (
    AttributeDefinition,
    AttributeDefinitions,
    AttributeType,
    DataWindow,
    Record,
    SelectResponse,
)

_ATTRIBUTE_TYPE_P2B = {}
_ATTRIBUTE_TYPE_P2B[AttributeTypeProto.INTEGER] = AttributeType.INTEGER
_ATTRIBUTE_TYPE_P2B[AttributeTypeProto.DECIMAL] = AttributeType.DECIMAL
_ATTRIBUTE_TYPE_P2B[AttributeTypeProto.STRING] = AttributeType.STRING
_ATTRIBUTE_TYPE_P2B[AttributeTypeProto.BLOB] = AttributeType.BLOB
_ATTRIBUTE_TYPE_P2B[AttributeTypeProto.BOOLEAN] = AttributeType.BOOLEAN


def _fetch_integer(record, index):
    return record.long_values[index]


def _fetch_decimal(record, index):
    return record.double_values[index]


def _fetch_string(record, index):
    return record.string_values[index]


def _fetch_blob(record, index):
    return record.blob_values[index]


def _fetch_boolean(record, index):
    return record.boolean_values[index]


_RECORD_FETCH = {}
_RECORD_FETCH[AttributeType.INTEGER] = _fetch_integer
_RECORD_FETCH[AttributeType.DECIMAL] = _fetch_decimal
_RECORD_FETCH[AttributeType.STRING] = _fetch_string
_RECORD_FETCH[AttributeType.BLOB] = _fetch_blob
_RECORD_FETCH[AttributeType.BOOLEAN] = _fetch_boolean


def decode_select_response(buffer):
    responses_src = SelectResponseProto()
    responses_src.ParseFromString(buffer)
    responses = []
    for response_src in responses_src.responses:
        definitions = AttributeDefinitions()
        for def_src in response_src.definitions:
            definition = AttributeDefinition(def_src.name, _ATTRIBUTE_TYPE_P2B.get(def_src.type))
            definitions.append(definition)

        data_windows = []
        for data_window_src in response_src.data:
            window_begin_time = data_window_src.window_begin_time
            records = []
            for record_src in data_window_src.records:
                if record_src.event_id:
                    event_id = uuid.UUID(bytes=record_src.event_id)
                else:
                    event_id = None
                timestamp = record_src.timestamp
                attributes = []
                for i in range(definitions.length()):
                    atype = definitions.get(i).get_type()
                    index = response_src.definitions[i].index_in_value_array
                    if atype is None:
                        attributes.append(None)
                    else:
                        attributes.append(_RECORD_FETCH.get(atype)(record_src, index))
                record = Record(
                    attributes,
                    definitions,
                    event_id=event_id,
                    timestamp=timestamp,
                )
                records.append(record)
            data_windows.append(DataWindow(window_begin_time, records))
        responses.append(SelectResponse(definitions, data_windows))

    return responses
