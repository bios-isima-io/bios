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


from enum import Enum, unique

from .._utils import check_not_empty, check_type
from .operation import Operation


@unique
class NodeType(Enum):
    """Enum class to be used for indicating node type."""

    SIGNAL = 1
    ROLLUP = 2
    ANALYSIS = 3

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.__repr__()


class UpdateEndpointsRequest:
    """tfos UpdateEndpointsRequest class"""

    def __init__(self, operation, endpoint, nodetype=None):
        check_type(operation, "operation", Operation)
        if nodetype:
            check_type(nodetype, "nodetype", NodeType)
        check_not_empty(endpoint, "endpoint")
        check_type(endpoint, "endpoint", str)
        self.operation = operation
        self.endpoint = endpoint
        self.nodetype = nodetype

    def __repr__(self):
        if self.nodetype:
            format_str = '{{"operation":"{}","endpoint":"{}","nodeType":"{}"}}'
            return format_str.format(self.operation, self.endpoint, self.nodetype)

        format_str = '{{"operation":"{}","endpoint":"{}"}}'
        return format_str.format(self.operation, self.endpoint)
