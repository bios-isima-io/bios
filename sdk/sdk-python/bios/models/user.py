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
from typing import List

from .._utils import check_not_empty_string, check_str_enum, check_str_list


class User:
    """Class to represent a user.

    Required parameters should be specified by the constructor as listed below. Optional parameters
    can be set by setter methods.

    Args:
        email (str): Email address of the user
        tenant_name (str): Name of the tenant that the user belongs to
        password (str): The user's password
        roles: (List[str]): User's roles. Allowed values are: SystemAdmin, TenantAdmin,
            SchemaExtractIngest, Extract, Ingest, and Report
    """

    _ROLES = {
        "SystemAdmin",
        "TenantAdmin",
        "SchemaExtractIngest",
        "Extract",
        "Ingest",
        "Report",
        "AppMaster",
    }
    _STATUSES = {"Active", "Suspended"}

    def __init__(
        self,
        email: str = None,
        full_name: str = None,
        tenant_name: str = None,
        password: str = None,
        roles: List[str] = None,
    ):
        check_not_empty_string(email, "email")
        check_not_empty_string(full_name, "full_name")
        check_not_empty_string(tenant_name, "tenant_name")
        check_not_empty_string(password, "password")
        check_str_list(roles, "roles", allow_empty=False, do_not_allow_duplicates=True)
        for i, role in enumerate(roles):
            check_str_enum(role, f"roles[{i}]", self._ROLES, True)

        self.tenant_name = tenant_name
        self.email = email
        self.full_name = full_name
        self.password = password
        self.roles = roles
        self.status = "Active"

    def set_status(self, status: str):
        """Specifies optional parameter 'status' (default is Active).

        Args:
            status (str): User's status. Allowed values are Active and Suspended

        Returns: User: Self
        """
        check_str_enum(status, "status", self._STATUSES, True)
        self.status = status
        return self

    def to_dict(self):
        """Converts the instance to a dictionary in the format acceptable by the server."""
        return {
            "tenantName": self.tenant_name,
            "email": self.email,
            "fullName": self.full_name,
            "password": self.password,
            "roles": self.roles,
            "status": self.status,
        }
