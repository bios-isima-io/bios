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

import time


class Time:
    """Utility class to provide time values as milliseconds"""

    @classmethod
    def now(cls) -> int:
        """Provides current time in milliseconds"""
        return int(time.time() * 1000)

    @classmethod
    def seconds(cls, value: int) -> int:
        """Converts the specified seconds to milliseconds"""
        return int(value * 1000)

    @classmethod
    def minutes(cls, value: int) -> int:
        """Converts the specified minutes to milliseconds"""
        return int(value * 60000)

    @classmethod
    def hours(cls, value: int) -> int:
        """Converts the specified hours to milliseconds"""
        return int(value * 3600000)

    @classmethod
    def days(cls, value: int) -> int:
        """Converts the specified days to milliseconds"""
        return int(value * 24 * 3600000)
