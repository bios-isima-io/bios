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

from .isql_response_message import ISqlResponse, ISqlResponseType


class InsertResponseRecord:
    """Plain python object used for returning Ingest API response.

    The response object has following properties:

    - event_id (UUID): Event ID
    - timestamp (int): Millisecond timestamp when ingest happened.
    """

    def __init__(self, proto_message=None, event_id=None, timestamp=None):
        if proto_message:
            self.event_id = uuid.UUID(bytes=proto_message.event_id)
            self.timestamp = proto_message.insert_timestamp
        else:
            self.event_id = event_id
            self.timestamp = timestamp

    def get_event_id(self):
        return self.event_id

    def get_timestamp(self):
        return self.timestamp

    def __repr__(self):
        return str(self.__dict__)


class InsertResponse(ISqlResponse):
    """Class to carry insert response"""

    def __init__(self, records: list):
        super().__init__(ISqlResponseType.INSERT)
        self.records = records

    def __repr__(self):
        return str(self.__dict__)
