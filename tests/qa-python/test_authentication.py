#!/usr/bin/env python3
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
import logging
import os
import sys
import time
import unittest
import pytest
import pprint

import bios
from bios import ServiceError
from bios.errors import ErrorCode

from setup_common import (
    BIOS_QA_COMMON_TENANT_NAME as TEST_TENANT_NAME,
    setup_tenant_config,
)
from tsetup import admin_pass, admin_user, sadmin_pass, sadmin_user
from tsetup import get_endpoint_url as ep_url


def test_login_success():
    with bios.login(ep_url(), sadmin_user, sadmin_pass) as session:
        assert session is not None
        pprint.pprint(session)


def test_login_failure_wrong_password():
    with pytest.raises(ServiceError) as excinfo:
        bios.login(ep_url(), admin_user, "wrongPassword")
    assert excinfo.value.error_code == ErrorCode.UNAUTHORIZED
    assert excinfo.value.message == "Authentication failed"


def test_login_failure_no_such_user():
    with pytest.raises(ServiceError) as excinfo:
        bios.login(ep_url(), "nosuchuser@example.com", "whatever")
    assert excinfo.value.error_code == ErrorCode.UNAUTHORIZED
    assert excinfo.value.message == "Authentication failed"


if __name__ == "__main__":
    pytest.main(sys.argv)
