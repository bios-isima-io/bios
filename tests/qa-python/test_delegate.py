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

import copy
import pprint
import sys

import bios
import pytest
from bios import ErrorCode, ServiceError
from setup_common import setup_tenant_config
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import ingest_pass, ingest_user, sadmin_pass, sadmin_user

APP_MASTER_TENANT_NAME = "delegateTest"
APP_MASTER_USER = f"app-master@{APP_MASTER_TENANT_NAME}"
APP_MASTER_PASSWORD = "app-master"

LESSER_APP_MASTER_USER = f"lesser-app-master@{APP_MASTER_TENANT_NAME}"
LESSER_APP_MASTER_PASSWORD = "lesser-app-master"

APP_TENANT_NAME = "appTenant"
NON_APP_TENANT_NAME = "nonAppTenant"

SIGNAL_SIMPLE_NAME = "simpleSignal"
SIGNAL_SIMPLE = {
    "signalName": SIGNAL_SIMPLE_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "one", "type": "String"},
        {"attributeName": "two", "type": "String"},
    ],
}

CONTEXT_SIMPLE_NAME = "simpleContext"
CONTEXT_SIMPLE = {
    "contextName": CONTEXT_SIMPLE_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "one", "type": "String"},
        {"attributeName": "two", "type": "String"},
    ],
    "primaryKey": ["one"],
}

APP_TENANT_ADMIN_USER = f"{admin_user}@{APP_TENANT_NAME}"
NON_APP_TENANT_ADMIN_USER = f"{admin_user}@{NON_APP_TENANT_NAME}"

CREATED_STREAMS = []


@pytest.fixture(scope="module")
def set_up_class():
    setup_tenant_config(clear_tenant=True, tenant_name=APP_MASTER_TENANT_NAME)
    setup_tenant_config(clear_tenant=True, tenant_name=NON_APP_TENANT_NAME)

    with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin_session:
        # app master without data read/write permission
        sadmin_session.create_user(
            bios.User(
                email=LESSER_APP_MASTER_USER,
                password=LESSER_APP_MASTER_PASSWORD,
                full_name="App master",
                tenant_name=APP_MASTER_TENANT_NAME,
                roles=["AppMaster"],
            )
        )

        # app master with data read/write permission to
        sadmin_session.create_user(
            bios.User(
                email=APP_MASTER_USER,
                password=APP_MASTER_PASSWORD,
                full_name="App master",
                tenant_name=APP_MASTER_TENANT_NAME,
                roles=["AppMaster", "SchemaExtractIngest"],
            )
        )

        try:
            sadmin_session.delete_tenant(APP_TENANT_NAME)
        except ServiceError as err:
            if err.error_code != ErrorCode.NO_SUCH_TENANT:
                raise
        sadmin_session.create_tenant(
            {
                "tenantName": APP_TENANT_NAME,
                "appMaster": APP_MASTER_TENANT_NAME,
            }
        )

    with bios.login(ep_url(), APP_TENANT_ADMIN_USER, admin_pass) as app_tenant_admin:
        app_tenant_admin.create_signal(SIGNAL_SIMPLE)
        app_tenant_admin.create_context(CONTEXT_SIMPLE)

    with bios.login(ep_url(), NON_APP_TENANT_ADMIN_USER, admin_pass) as non_app_tenant_admin:
        non_app_tenant_admin.create_signal(SIGNAL_SIMPLE)
        non_app_tenant_admin.create_context(CONTEXT_SIMPLE)

    with bios.login(ep_url(), f"{admin_user}@{APP_MASTER_TENANT_NAME}", admin_pass) as admin:
        signal = copy.deepcopy(SIGNAL_SIMPLE)
        signal["signalName"] = "masterSimpleSignal"
        admin.create_signal(signal)
        context = copy.deepcopy(CONTEXT_SIMPLE)
        context["contextName"] = "masterSimpleContext"
        admin.create_context(context)


def test_signal_insertion(set_up_class):
    """Tests delegated single insertion"""
    with bios.login(ep_url(), APP_MASTER_USER, APP_MASTER_PASSWORD) as master_session:
        statement = bios.isql().insert().into(SIGNAL_SIMPLE_NAME).csv("hello,world").build()
        response = master_session.for_tenant(APP_TENANT_NAME).execute(statement)
        insert_timestamp = response.records[0].timestamp
        with pytest.raises(ServiceError) as exc_info:
            master_session.for_tenant(NON_APP_TENANT_NAME).execute(statement)
        assert exc_info.value.error_code == ErrorCode.FORBIDDEN
        with pytest.raises(ServiceError) as exc_info:
            master_session.for_tenant("nonExistingTenant").execute(statement)
        assert exc_info.value.error_code == ErrorCode.NO_SUCH_TENANT

    delta = bios.time.now() - insert_timestamp
    statement = (
        bios.isql()
        .select()
        .from_signal(SIGNAL_SIMPLE_NAME)
        .time_range(insert_timestamp, delta)
        .build()
    )

    with bios.login(ep_url(), APP_TENANT_ADMIN_USER, admin_pass) as app_session:
        response = app_session.execute(statement)
        print(response)
        assert len(response.get_data_windows()[0].records) == 1
        record = response.get_data_windows()[0].get_records()[0]
        assert record.get("one") == "hello"
        assert record.get("two") == "world"

    with bios.login(ep_url(), NON_APP_TENANT_ADMIN_USER, admin_pass) as non_app_session:
        response = non_app_session.execute(statement)
        assert len(response.get_data_windows()[0].records) == 0

    with bios.login(ep_url(), f"{admin_user}@{APP_MASTER_TENANT_NAME}", admin_pass) as session:
        with pytest.raises(ServiceError) as exc_info:
            session.execute(statement)
        assert exc_info.value.error_code == ErrorCode.NO_SUCH_STREAM


def test_bulk_insertion(set_up_class):
    """Tests delegated bulk insertion"""
    with bios.login(ep_url(), APP_MASTER_USER, APP_MASTER_PASSWORD) as master_session:
        statement = (
            bios.isql().insert().into(SIGNAL_SIMPLE_NAME).csv_bulk(["111,222", "aaa,bbb"]).build()
        )
        response = master_session.for_tenant(APP_TENANT_NAME).execute(statement)
        insert_timestamp = response.records[0].timestamp

        with pytest.raises(ServiceError) as exc_info:
            master_session.for_tenant(NON_APP_TENANT_NAME).execute(statement)
        assert exc_info.value.error_code == ErrorCode.FORBIDDEN

        with pytest.raises(ServiceError) as exc_info:
            master_session.for_tenant("nonExistingTenant").execute(statement)
        assert exc_info.value.error_code == ErrorCode.NO_SUCH_TENANT

    delta = bios.time.now() - insert_timestamp
    statement = (
        bios.isql()
        .select()
        .from_signal(SIGNAL_SIMPLE_NAME)
        .time_range(insert_timestamp, delta)
        .build()
    )

    # data went to the delegated tenant
    with bios.login(ep_url(), APP_TENANT_ADMIN_USER, admin_pass) as app_session:
        response = app_session.execute(statement)
        print(response)
        assert len(response.get_data_windows()[0].records) == 2
        records = response.get_data_windows()[0].get_records()
        assert records[0].get("one") == "111"
        assert records[0].get("two") == "222"
        assert records[1].get("one") == "aaa"
        assert records[1].get("two") == "bbb"

    # and not to the non-app tenant
    with bios.login(ep_url(), NON_APP_TENANT_ADMIN_USER, admin_pass) as non_app_session:
        response = non_app_session.execute(statement)
        assert len(response.get_data_windows()[0].records) == 0

    # nor to the app-master's tenant
    with bios.login(ep_url(), f"{admin_user}@{APP_MASTER_TENANT_NAME}", admin_pass) as session:
        with pytest.raises(ServiceError) as exc_info:
            session.execute(statement)
        assert exc_info.value.error_code == ErrorCode.NO_SUCH_STREAM


def test_upserts(set_up_class):
    """Tests delegated context upserts"""
    with bios.login(ep_url(), APP_MASTER_USER, APP_MASTER_PASSWORD) as master_session:
        statement = (
            bios.isql().upsert().into(CONTEXT_SIMPLE_NAME).csv_bulk(["111,222", "aaa,bbb"]).build()
        )
        master_session.for_tenant(APP_TENANT_NAME).execute(statement)

        with pytest.raises(ServiceError) as exc_info:
            master_session.for_tenant(NON_APP_TENANT_NAME).execute(statement)
        assert exc_info.value.error_code == ErrorCode.FORBIDDEN

        with pytest.raises(ServiceError) as exc_info:
            master_session.for_tenant("nonExistingTenant").execute(statement)
        assert exc_info.value.error_code == ErrorCode.NO_SUCH_TENANT

    statement = (
        bios.isql()
        .select()
        .from_context(CONTEXT_SIMPLE_NAME)
        .where(keys=[["111"], ["aaa"]])
        .build()
    )

    # data went to the delegated tenant
    with bios.login(ep_url(), APP_TENANT_ADMIN_USER, admin_pass) as app_session:
        response = app_session.execute(statement)
        print(response)
        assert len(response.get_records()) == 2
        records = response.get_records()
        assert records[0].get("one") == "111"
        assert records[0].get("two") == "222"
        assert records[1].get("one") == "aaa"
        assert records[1].get("two") == "bbb"

    # and not to the non-app tenant
    with bios.login(ep_url(), NON_APP_TENANT_ADMIN_USER, admin_pass) as non_app_session:
        response = non_app_session.execute(statement)
        assert len(response.get_records()) == 0

    # nor to the app-master's tenant
    with bios.login(ep_url(), f"{admin_user}@{APP_MASTER_TENANT_NAME}", admin_pass) as session:
        with pytest.raises(ServiceError) as exc_info:
            session.execute(statement)
        assert exc_info.value.error_code == ErrorCode.NO_SUCH_STREAM


def test_app_master_role(set_up_class):
    """Only AppMaster can delegate insertions"""
    statement = bios.isql().insert().into(SIGNAL_SIMPLE_NAME).csv("hello,world").build()
    users = [
        (f"{admin_user}@{APP_MASTER_TENANT_NAME}", admin_pass),
        (f"{ingest_user}@{APP_MASTER_TENANT_NAME}", ingest_pass),
    ]
    for user, password in users:
        with bios.login(ep_url(), user, password) as session:
            with pytest.raises(ServiceError) as exc_info:
                session.for_tenant(APP_TENANT_NAME).execute(statement)
            assert exc_info.value.error_code == ErrorCode.FORBIDDEN


def test_app_master_for_own_tenant_signal(set_up_class):
    """When not delegating, app master follows the original permissions"""
    statement = bios.isql().insert().into("masterSimpleSignal").csv("hello,world").build()

    # the lesser app master does not have data permission for its own
    with bios.login(ep_url(), LESSER_APP_MASTER_USER, LESSER_APP_MASTER_PASSWORD) as session:
        with pytest.raises(ServiceError) as exc_info:
            session.execute(statement)
        assert exc_info.value.error_code == ErrorCode.FORBIDDEN

    # but the app master does
    with bios.login(ep_url(), APP_MASTER_USER, APP_MASTER_PASSWORD) as session:
        response = session.execute(statement)
        insert_timestamp = response.records[0].timestamp
        select_statement = (
            bios.isql()
            .select()
            .from_signal("masterSimpleSignal")
            .time_range(insert_timestamp, bios.time.now() - insert_timestamp)
            .build()
        )
        response = session.execute(select_statement)
        record = response.get_data_windows()[0].get_records()[0]
        assert record.get("one") == "hello"
        assert record.get("two") == "world"


def test_app_master_for_own_tenant_context(set_up_class):
    """When not delegating, app master follows the original permissions"""
    statement = bios.isql().upsert().into("masterSimpleContext").csv("hello,world").build()

    # the lesser app master does not have data permission for its own
    with bios.login(ep_url(), LESSER_APP_MASTER_USER, LESSER_APP_MASTER_PASSWORD) as session:
        with pytest.raises(ServiceError) as exc_info:
            session.execute(statement)
        assert exc_info.value.error_code == ErrorCode.FORBIDDEN

    # but the app master does
    with bios.login(ep_url(), APP_MASTER_USER, APP_MASTER_PASSWORD) as session:
        session.execute(statement)
        select_statement = (
            bios.isql()
            .select()
            .from_context("masterSimpleContext")
            .where(keys=[["hello"]])
            .build()
        )
        response = session.execute(select_statement)
        record = response.get_records()[0]
        assert record.get("one") == "hello"
        assert record.get("two") == "world"


def test_delegate_signal_admin(set_up_class):
    """Let app master manage signals in behalf of an app tenant"""

    # the lesser app master does not have data permission for its own
    with (
        bios.login(ep_url(), APP_MASTER_USER, APP_MASTER_PASSWORD) as master_session,
        bios.login(ep_url(), f"{admin_user}@{APP_MASTER_TENANT_NAME}", admin_pass) as master_admin,
        bios.login(ep_url(), APP_TENANT_ADMIN_USER, admin_pass) as tenant_session,
    ):
        signal_name = "tempSignal"
        signal = {
            "signalName": signal_name,
            "missingAttributePolicy": "Reject",
            "attributes": [{"attributeName": "ichi", "type": "Integer"}],
        }
        master_session.for_tenant(APP_TENANT_NAME).create_signal(signal)
        from_tenant = tenant_session.get_signal(signal_name)
        from_master = master_session.for_tenant(APP_TENANT_NAME).get_signal(signal_name)
        assert from_tenant == from_master
        with pytest.raises(ServiceError) as exc_info:
            master_admin.get_signal(signal_name)
        assert exc_info.value.error_code == ErrorCode.NO_SUCH_STREAM

        signal["attributes"].append(
            {"attributeName": "ni", "type": "String", "default": "MISSING"}
        )
        master_session.for_tenant(APP_TENANT_NAME).update_signal(signal_name, signal)
        from_tenant = tenant_session.get_signals(names=[signal_name], detail=True)
        from_master = master_session.for_tenant(APP_TENANT_NAME).get_signals(
            names=[signal_name], detail=True
        )
        assert from_tenant == from_master

        master_session.for_tenant(APP_TENANT_NAME).delete_signal(signal_name)
        with pytest.raises(ServiceError) as exc_info:
            tenant_session.get_signal(signal_name)
        with pytest.raises(ServiceError) as exc_info:
            master_session.for_tenant(APP_TENANT_NAME).get_signal(signal_name)
        assert exc_info.value.error_code == ErrorCode.NO_SUCH_STREAM


def test_delegate_context_admin(set_up_class):
    """Let app master manage contexts in behalf of an app tenant"""

    # the lesser app master does not have data permission for its own
    with (
        bios.login(ep_url(), APP_MASTER_USER, APP_MASTER_PASSWORD) as master_session,
        bios.login(ep_url(), f"{admin_user}@{APP_MASTER_TENANT_NAME}", admin_pass) as master_admin,
        bios.login(ep_url(), APP_TENANT_ADMIN_USER, admin_pass) as tenant_session,
    ):
        context_name = "tempContext"
        context = {
            "contextName": context_name,
            "missingAttributePolicy": "Reject",
            "attributes": [{"attributeName": "ichi", "type": "Integer"}],
            "primaryKey": ["ichi"],
        }
        master_session.for_tenant(APP_TENANT_NAME).create_context(context)
        from_tenant = tenant_session.get_context(context_name)
        from_master = master_session.for_tenant(APP_TENANT_NAME).get_context(context_name)
        assert from_tenant == from_master
        with pytest.raises(ServiceError) as exc_info:
            master_admin.get_context(context_name)
        assert exc_info.value.error_code == ErrorCode.NO_SUCH_STREAM

        context["attributes"].append(
            {"attributeName": "ni", "type": "String", "default": "MISSING"}
        )
        master_session.for_tenant(APP_TENANT_NAME).update_context(context_name, context)
        from_tenant = tenant_session.get_contexts(names=[context_name], detail=True)
        from_master = master_session.for_tenant(APP_TENANT_NAME).get_contexts(
            names=[context_name], detail=True
        )
        assert from_tenant == from_master

        master_session.for_tenant(APP_TENANT_NAME).delete_context(context_name)
        with pytest.raises(ServiceError) as exc_info:
            tenant_session.get_context(context_name)
        with pytest.raises(ServiceError) as exc_info:
            master_session.for_tenant(APP_TENANT_NAME).get_context(context_name)
        assert exc_info.value.error_code == ErrorCode.NO_SUCH_STREAM


def test_delegate_signal_admin_denied(set_up_class):
    """Let app master manage signals in behalf of an non-app tenant"""

    # the lesser app master does not have data permission for its own
    with bios.login(ep_url(), APP_MASTER_USER, APP_MASTER_PASSWORD) as master_session:
        signal_name = "tempSignal"
        signal = {
            "signalName": signal_name,
            "missingAttributePolicy": "Reject",
            "attributes": [{"attributeName": "ichi", "type": "Integer"}],
        }
        with pytest.raises(ServiceError) as exc_info:
            master_session.for_tenant(NON_APP_TENANT_NAME).create_signal(signal)
        assert exc_info.value.error_code == ErrorCode.FORBIDDEN

        with pytest.raises(ServiceError) as exc_info:
            master_session.for_tenant(APP_MASTER_TENANT_NAME).create_signal(signal)
        assert exc_info.value.error_code == ErrorCode.FORBIDDEN

        with pytest.raises(ServiceError) as exc_info:
            master_session.for_tenant(NON_APP_TENANT_NAME).get_signal(signal_name)
        assert exc_info.value.error_code == ErrorCode.FORBIDDEN

        with pytest.raises(ServiceError) as exc_info:
            master_session.for_tenant(APP_MASTER_TENANT_NAME).get_signal(signal_name)
        assert exc_info.value.error_code == ErrorCode.FORBIDDEN

        with pytest.raises(ServiceError) as exc_info:
            master_session.for_tenant(NON_APP_TENANT_NAME).update_signal(signal_name, signal)
        assert exc_info.value.error_code == ErrorCode.FORBIDDEN

        with pytest.raises(ServiceError) as exc_info:
            master_session.for_tenant(APP_MASTER_TENANT_NAME).update_signal(signal_name, signal)
        assert exc_info.value.error_code == ErrorCode.FORBIDDEN

        with pytest.raises(ServiceError) as exc_info:
            master_session.for_tenant(NON_APP_TENANT_NAME).delete_signal(signal_name)
        assert exc_info.value.error_code == ErrorCode.FORBIDDEN

        with pytest.raises(ServiceError) as exc_info:
            master_session.for_tenant(APP_MASTER_TENANT_NAME).delete_signal(signal_name)
        assert exc_info.value.error_code == ErrorCode.FORBIDDEN


def test_delegate_context_admin_denied(set_up_class):
    """Let app master manage contexts in behalf of an non-app tenant"""

    # the lesser app master does not have data permission for its own
    with bios.login(ep_url(), APP_MASTER_USER, APP_MASTER_PASSWORD) as master_session:
        context_name = "tempContext"
        context = {
            "contextName": context_name,
            "missingAttributePolicy": "Reject",
            "attributes": [{"attributeName": "ichi", "type": "Integer"}],
            "primaryKey": ["ichi"],
        }
        with pytest.raises(ServiceError) as exc_info:
            master_session.for_tenant(NON_APP_TENANT_NAME).create_context(context)
        assert exc_info.value.error_code == ErrorCode.FORBIDDEN

        with pytest.raises(ServiceError) as exc_info:
            master_session.for_tenant(APP_MASTER_TENANT_NAME).create_context(context)
        assert exc_info.value.error_code == ErrorCode.FORBIDDEN

        with pytest.raises(ServiceError) as exc_info:
            master_session.for_tenant(NON_APP_TENANT_NAME).get_context(context_name)
        assert exc_info.value.error_code == ErrorCode.FORBIDDEN

        with pytest.raises(ServiceError) as exc_info:
            master_session.for_tenant(APP_MASTER_TENANT_NAME).get_context(context_name)
        assert exc_info.value.error_code == ErrorCode.FORBIDDEN

        with pytest.raises(ServiceError) as exc_info:
            master_session.for_tenant(NON_APP_TENANT_NAME).update_context(context_name, context)
        assert exc_info.value.error_code == ErrorCode.FORBIDDEN

        with pytest.raises(ServiceError) as exc_info:
            master_session.for_tenant(APP_MASTER_TENANT_NAME).update_context(context_name, context)
        assert exc_info.value.error_code == ErrorCode.FORBIDDEN

        with pytest.raises(ServiceError) as exc_info:
            master_session.for_tenant(NON_APP_TENANT_NAME).delete_context(context_name)
        assert exc_info.value.error_code == ErrorCode.FORBIDDEN

        with pytest.raises(ServiceError) as exc_info:
            master_session.for_tenant(APP_MASTER_TENANT_NAME).delete_context(context_name)
        assert exc_info.value.error_code == ErrorCode.FORBIDDEN


if __name__ == "__main__":
    pytest.main(sys.argv)
