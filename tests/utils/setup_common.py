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

import bios
import pytest
from bios import Client, ErrorCode, ServiceError
from tsetup import admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import sadmin_pass, sadmin_user

BIOS_QA_COMMON_TENANT_NAME = "biosPythonQA"
BIOS_QA_COMMON_ADMIN_USER = admin_user + "@" + BIOS_QA_COMMON_TENANT_NAME


def setup_tenant_config(clear_tenant=False, tenant_name=None):
    if not tenant_name:
        tenant_name = BIOS_QA_COMMON_TENANT_NAME
    with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
        if clear_tenant:
            try:
                sadmin.delete_tenant(tenant_name)
            except bios.ServiceError:
                pass
        try:
            sadmin.create_tenant({"tenantName": tenant_name})
            print("created tenant :", tenant_name)
            users = sadmin.get_users([f"{admin_user}@{tenant_name}"])
            sadmin.modify_user(users["users"][0].get("userId"), roles=["TenantAdmin", "Report"])
        except bios.ServiceError:
            if clear_tenant:
                raise
            else:
                pass


def read_file(jsonFilePath):
    with open(jsonFilePath) as stream_config_file:
        stream_config = stream_config_file.read()
    stream_config_file.close()
    return stream_config


def setup_signal(session: Client, signal_config: dict, clear_signal: bool = True) -> dict:
    """Setup a test signal.
    Args:
        session (Client): Bios session
        signal_config (Union[dict, str]): Signal configuration to add
        clear_signal (bool = True): Delete signal before creating the signal if true
    Returns: dict: Created signal config returned by the server
    """
    if clear_signal:
        try:
            session.delete_signal(signal_config.get("signalName"))
        except ServiceError as e:
            if e.error_code is not ErrorCode.NO_SUCH_STREAM:
                raise
        return session.create_signal(signal_config)


def setup_context(session: Client, context_config: dict, clear_context: bool = True) -> dict:
    """Setup a test context.
    Args:
        session (Client): Bios session
        context_config (Union[dict, str]): Context configuration to add
        clear_context (bool = True): Delete context before creating the context if true
    Returns: dict: Created context config returned by the server
    """
    if clear_context:
        try:
            session.delete_context(context_config.get("contextName"))
        except ServiceError as e:
            if e.error_code is not ErrorCode.NO_SUCH_STREAM:
                raise
        return session.create_context(context_config)


def try_delete_signal(session: Client, signal_name: str):
    """Try deleting signal ignoring error due to missing the signal"""
    try_delete_stream(session, signal_name, "signal")


def try_delete_context(session: Client, context_name: str):
    """Try deleting context ignoring error due to missing the context"""
    try_delete_stream(session, context_name, "context")


def try_delete_stream(session: Client, stream_name: str, stream_type: str):
    """Try deleting signal or context ignoring errors due to missing stream
    Args:
        session (Client): biOS session
        stream_name (str): Signal or context name
        stream_type (str): Specifies 'signal' or 'context'
    """
    try:
        if stream_type == "signal":
            session.delete_signal(stream_name)
        else:
            session.delete_context(stream_name)
    except ServiceError:
        pass


def update_stream_expect_error(session, stream_type, stream_name, stream, error_code, error):
    exception_thrown = None
    with pytest.raises(ServiceError) as exc_info:
        if stream_type == "signal":
            session.update_signal(stream_name, stream)
        else:
            session.update_context(stream_name, stream)
    exception_thrown = exc_info.value
    assert exception_thrown.error_code == error_code
    assert error in exception_thrown.message


# create JavaSDK Test Config
if __name__ == "__main__":
    setup_tenant_config(True)
    print("done")
