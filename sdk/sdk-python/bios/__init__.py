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

"""
SDK provides a way to access biOS servers. It connects to the servers and sends requests
using API methods. The SDK takes care of managing the underlying communication layers, so
users can focus on the application's business logic via the API methods.
"""

import sys as _sys

from ._version import __version__
from .bios_time import Time as time
from .client import Client, disable_threads, enable_threads, login
from .errors import ErrorCode, InsertBulkError, ServiceError
from .isql_request import isql, rollback_on_failure
from .models import AppType, NodeType, User

if _sys.version_info < (3, 6):
    raise ImportError("bios only supports Python 3.6 or higher.")


__all__ = [
    "AppType",
    "Client",
    "disable_threads",
    "enable_threads",
    "ErrorCode",
    "InsertBulkError",
    "isql",
    "login",
    "NodeType",
    "rollback_on_failure",
    "ServiceError",
    "time",
    "User",
]
