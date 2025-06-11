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
import re

from .._utils import check_type
from ..errors import ErrorCode, ServiceError
from .metric_function import MetricFunction


class Metric:
    """Bios models metric class"""

    pattern = re.compile("([a-zA-Z0-9][a-zA-Z0-9_]+)\\((.*)\\)")
    function_from_name = {}
    for function in MetricFunction:
        function_from_name[function.name] = function

    def __init__(self, metric):
        check_type(metric, "metric", str)
        (
            self.function,
            self.of,
            self.name,
        ) = self._parse_metric_string(metric)
        self._as = None

    def set_as(self, value):
        self._as = value
        return self

    def get_as(self):
        return self._as

    def __repr__(self):
        obj = self.__dict__.copy()
        if obj.get("function") is not None:
            obj["function"] = str(obj.get("function"))
        if self._as is not None:
            obj["as"] = self._as
            del obj["_as"]
        return json.dumps(obj)

    def _parse_metric_string(self, metric):
        mat = self.pattern.match(metric)
        if not mat:
            return (None, None, metric.strip())
        function_src = mat.group(1).strip().upper()
        domain = mat.group(2).strip()
        if function_src == "COUNT":
            if len(domain) > 0:
                raise ServiceError(
                    ErrorCode.INVALID_ARGUMENT,
                    "count() function must not take parameter",
                )
            return (MetricFunction.COUNT, None, None)

        if len(domain) == 0:
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT,
                f"{function_src} function must specify a parameter",
            )

        if function_src in self.function_from_name:
            function = self.function_from_name[function_src]
        else:
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT,
                f"{function_src}: unsupported function",
            )
        return (function, domain, None)
