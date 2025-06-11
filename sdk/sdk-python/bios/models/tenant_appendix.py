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

from enum import Enum, auto


class TenantAppendixCategory(Enum):
    """Enum that represents tenant appendix category"""

    IMPORT_SOURCES = auto()
    IMPORT_DESTINATIONS = auto()
    IMPORT_FLOW_SPECS = auto()
    IMPORT_DATA_PROCESSORS = auto()


class ImportDataMappingType(Enum):
    """Enum that represents type of import data mapper"""

    JSON = auto()
    CSV = auto()
