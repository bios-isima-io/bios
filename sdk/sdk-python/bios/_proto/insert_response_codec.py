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

from .._proto._bios import data_pb2
from ..models import InsertResponseRecord


def decode_insert_response_record(
    buffer: bytes,
) -> InsertResponseRecord:
    """Decodes insert response protocol buffer byte array
    Args:
        buffer (bytes): Protocol buffer serialized insert response record

    Returns:
        InsertResponseRecord: Insert response record
    """
    resp_proto = data_pb2.InsertSuccessResponse()
    resp_proto.ParseFromString(buffer)
    return InsertResponseRecord(proto_message=resp_proto)
