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


@unique
class CSdkOperationId(Enum):
    """Enum class to be used for indicating update operation type."""

    def __new__(cls):
        value = len(cls.__members__)
        obj = object.__new__(cls)
        obj._value_ = value
        return obj

    NOOP = ()
    GET_VERSION = ()
    START_SESSION = ()
    RENEW_SESSION = ()
    SELECT_CONTEXT_ENTRIES = ()
    LIST_ENDPOINTS = ()
    LIST_CONTEXT_ENDPOINTS = ()
    UPDATE_ENDPOINTS = ()
    GET_PROPERTY = ()
    SET_PROPERTY = ()
    GET_UPSTREAM_CONFIG = ()
    SELECT_PROTO = ()
    LOGIN_BIOS = ()
    RESET_PASSWORD = ()
    GET_SIGNALS = ()
    CREATE_SIGNAL = ()
    UPDATE_SIGNAL = ()
    DELETE_SIGNAL = ()
    GET_CONTEXTS = ()
    GET_CONTEXT = ()
    CREATE_CONTEXT = ()
    UPDATE_CONTEXT = ()
    DELETE_CONTEXT = ()
    MULTI_GET_CONTEXT_ENTRIES_BIOS = ()
    GET_CONTEXT_ENTRIES_BIOS = ()
    CREATE_CONTEXT_ENTRY_BIOS = ()
    UPDATE_CONTEXT_ENTRY_BIOS = ()
    DELETE_CONTEXT_ENTRY_BIOS = ()
    GET_TENANTS_BIOS = ()
    GET_TENANT_BIOS = ()
    CREATE_TENANT_BIOS = ()
    UPDATE_TENANT_BIOS = ()
    DELETE_TENANT_BIOS = ()
    LIST_APP_TENANTS = ()
    INSERT_PROTO = ()
    INSERT_BULK_PROTO = ()
    REPLACE_CONTEXT_ENTRY_BIOS = ()
    CREATE_EXPORT_DESTINATION = ()
    GET_EXPORT_DESTINATION = ()
    UPDATE_EXPORT_DESTINATION = ()
    DELETE_EXPORT_DESTINATION = ()
    DATA_EXPORT_START = ()
    DATA_EXPORT_STOP = ()
    CREATE_TENANT_APPENDIX = ()
    GET_TENANT_APPENDIX = ()
    UPDATE_TENANT_APPENDIX = ()
    DELETE_TENANT_APPENDIX = ()
    DISCOVER_IMPORT_SUBJECTS = ()
    CREATE_USER = ()
    GET_USERS = ()
    MODIFY_USER = ()
    DELETE_USER = ()
    REGISTER_APPS_SERVICE = ()
    GET_APPS_INFO = ()
    DEREGISTER_APPS_SERVICE = ()
    MAINTAIN_KEYSPACES = ()
    MAINTAIN_TABLES = ()
    MAINTAIN_CONTEXT = ()
    GET_REPORT_CONFIGS = ()
    PUT_REPORT_CONFIG = ()
    DELETE_REPORT = ()
    GET_INSIGHT_CONFIGS = ()
    PUT_INSIGHT_CONFIGS = ()
    DELETE_INSIGHT_CONFIGS = ()
    FEATURE_STATUS = ()
    FEATURE_REFRESH = ()
    GET_ALL_CONTEXT_SYNOPSES = ()
    GET_CONTEXT_SYNOPSIS = ()
    REGISTER_FOR_SERVICE = ()
    STORES_QUERY = ()
    TEST = ()
    TEST_BIOS = ()
    TEST_LOG = ()
    END = ()

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.__repr__()
