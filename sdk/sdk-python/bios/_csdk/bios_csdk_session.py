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

import math
import time
import uuid
from typing import Any, List, Tuple, Union

from .._csdk.insert_bulk_client import InsertBulkClient
from .._proto._bios import data_pb2
from .._proto.insert_response_codec import decode_insert_response_record
from .._proto.select_response_codec import decode_select_response
from .._utils import check_list, check_type
from ..errors import ErrorCode, ServiceError
from ..isql_delete_request import ISqlDeleteContext
from ..isql_insert_request import ISqlInsertCompleteRequest
from ..isql_select_request import (
    ISqlSelectFromContext,
    ISqlSelectRequestMessage,
    ISqlSelectRequestMessageBuilderEx,
    MultiGetContextEntriesRequest,
)
from ..isql_update_context_request import ISqlUpdateContext
from ..isql_upsert_request import ISqlUpsertCompleteRequest
from ..models import (
    InsertResponse,
    Operation,
    TenantAppendixCategory,
    UpdateEndpointsRequest,
)
from ..models.isql_response_message import EmptyISqlResponse, ISqlResponseType
from ..models.select_response import ContextRecords, SelectContextResponse
from ._error_handler import _ErrorHandler
from .csdk_session import CSdkSession
from .ingest_bulk_handler import IngestBulkHandler
from .operation_id import CSdkOperationId


class BiosCSdkSession(CSdkSession):
    """
    BIOS session implementation over C-SDK.
    """

    def __init__(self):
        super().__init__(error_handler=_ErrorHandler())

    def login(self, email, password, app_name, app_type):
        """API method to login to a user.

        Args:
            email (str): user email
            password (str): Password
        """
        self._run(self._login_async(email, password, app_name, app_type))

    async def _login_async(self, email, password, app_name, app_type, disable_routing=False):
        """Internal method that calls Login C-SDK operation."""
        wrapper_start = time.time()
        (seqno, future) = self._create_future()
        cred = {
            "email": email,
            "password": password,
        }
        if app_name:
            cred["appName"] = app_name
        if app_type:
            cred["appType"] = str(app_type)
        credentials = self._make_payload(cred)
        self._logger.debug("credentials=%s", credentials)
        self._csdk.login_bios(
            self._session_handle,
            seqno,
            self._session_id,
            credentials,
            disable_routing,
        )
        response = await future

        wrapper_elapsed = math.ceil((time.time() - wrapper_start) * 1000 * 1000)
        reply = self._handle_response(response[0], response[1], response[2])
        self._tenant_name = reply.get("tenant")
        self._app_name = reply.get("appName")
        self._app_type = reply.get("appType")
        self._csdk.log(
            self._session_handle,
            self._session_id,
            response[3],
            response[4],
            wrapper_elapsed,
            0,
            0,
            str(CSdkOperationId.LOGIN_BIOS),
            "OK",
            "email=" + email,
            response[2] or "",
            "",
            0,
            0,
            0,
        )

    def get_signals(
        self,
        names=None,
        detail=False,
        include_internal=False,
        inferred_tags=False,
        tenant_name=None,
    ):
        if names is not None:
            check_type(names, "names", list)
            for name in names:
                check_type(name, "name", str)
        if detail is not None:
            check_type(detail, "detail", bool)
        if include_internal is not None:
            check_type(include_internal, "include_internal", bool)
        if inferred_tags is not None:
            check_type(inferred_tags, "inferred_tags", bool)

        resources = {"tenant": self._tenant_name}
        options = {}
        if names:
            options["names"] = ",".join(names)
        if detail:
            options["detail"] = "true"
        if include_internal:
            options["includeInternal"] = "true"
        if inferred_tags:
            options["includeInferredTags"] = "true"
        if tenant_name:
            resources["tenant"] = tenant_name
            options["delegate"] = "true"
        return self.generic_method(
            CSdkOperationId.GET_SIGNALS, resources=resources, options=options
        )[0]

    def create_signal(self, signal, tenant_name=None):
        check_type(signal, "signal", [dict, str])
        resources = {"tenant": self._tenant_name}
        options = {}
        if tenant_name:
            resources["tenant"] = tenant_name
            options["delegate"] = "true"
        return self.generic_method(
            CSdkOperationId.CREATE_SIGNAL,
            resources=resources,
            options=options,
            data=signal,
            timeout=0,  # no timeout
        )[0]

    def update_signal(self, signal_name, signal_config, tenant_name=None):
        check_type(signal_name, "signal_name", str)
        check_type(signal_config, "signal_config", [dict, str])
        resources = {"tenant": self._tenant_name, "stream": signal_name}
        options = {}
        if tenant_name:
            resources["tenant"] = tenant_name
            options["delegate"] = "true"
        return self.generic_method(
            CSdkOperationId.UPDATE_SIGNAL, resources=resources, options=options, data=signal_config
        )[0]

    def delete_signal(self, signal_name, tenant_name=None):
        check_type(signal_name, "signal_name", str)
        resources = {"tenant": self._tenant_name, "stream": signal_name}
        options = {}
        if tenant_name:
            resources["tenant"] = tenant_name
            options["delegate"] = "true"
        return self.generic_method(
            CSdkOperationId.DELETE_SIGNAL, resources=resources, options=options
        )[0]

    def get_contexts(
        self,
        names=None,
        detail=False,
        include_internal=False,
        inferred_tags=False,
        tenant_name=None,
    ):
        if names is not None:
            check_type(names, "names", [str, list])
        if detail is not None:
            check_type(detail, "detail", bool)
        if include_internal is not None:
            check_type(include_internal, "include_internal", bool)
        if inferred_tags is not None:
            check_type(inferred_tags, "inferred_tags", bool)

        options = {}
        if names:
            options["names"] = ",".join(names)
        if detail:
            options["detail"] = "true"
        if include_internal:
            options["includeInternal"] = "true"
        if inferred_tags:
            options["includeInferredTags"] = "true"
        resources = {"tenant": self._tenant_name}
        if tenant_name:
            resources["tenant"] = tenant_name
            options["delegate"] = "true"
        return self.generic_method(
            CSdkOperationId.GET_CONTEXTS, resources=resources, options=options
        )[0]

    def create_context(self, context, tenant_name=None):
        check_type(context, "context", [str, dict])
        resources = {"tenant": self._tenant_name}
        options = {}
        if tenant_name:
            resources["tenant"] = tenant_name
            options["delegate"] = "true"
        return self.generic_method(
            CSdkOperationId.CREATE_CONTEXT,
            resources=resources,
            options=options,
            data=context,
            timeout=0,  # no timeout
        )[0]

    def update_context(self, context_name, context_config, tenant_name=None):
        check_type(context_name, "context_name", str)
        check_type(context_config, "context", [str, dict])

        resources = {"tenant": self._tenant_name, "stream": context_name}
        options = {}
        if tenant_name:
            resources["tenant"] = tenant_name
            options["delegate"] = "true"

        return self.generic_method(
            CSdkOperationId.UPDATE_CONTEXT,
            resources=resources,
            options=options,
            data=context_config,
        )[0]

    def delete_context(self, context_name, tenant_name=None):
        check_type(context_name, "context_name", str)
        resources = {"tenant": self._tenant_name, "stream": context_name}
        options = {}
        if tenant_name:
            resources["tenant"] = tenant_name
            options["delegate"] = "true"
        return self.generic_method(
            CSdkOperationId.DELETE_CONTEXT, resources=resources, options=options
        )[0]

    def get_all_context_synopses(self, current_time):
        request = {"currentTime": current_time}
        return self.simple_method(
            CSdkOperationId.GET_ALL_CONTEXT_SYNOPSES, tenant=self._tenant_name, data=request
        )[0]

    def get_context_synopsis(self, context_name, current_time):
        request = {"currentTime": current_time}
        return self.simple_method(
            CSdkOperationId.GET_CONTEXT_SYNOPSIS,
            tenant=self._tenant_name,
            stream=context_name,
            data=request,
        )[0]

    def get_tenant(self, tenant, detail=False, include_internal=False, inferred_tags=False):
        resources = {"tenantName": tenant if tenant else self._tenant_name}
        options = {}
        if detail:
            options["detail"] = "true"
        if include_internal:
            options["includeInternal"] = "true"
        if inferred_tags:
            options["includeInferredTags"] = "true"
        return self.generic_method(
            CSdkOperationId.GET_TENANT_BIOS,
            resources,
            options=options,
        )[0]

    def list_tenants(self, names=None):
        options = {}
        if names:
            options["names"] = ",".join(names)
        response = self.generic_method(CSdkOperationId.GET_TENANTS_BIOS, options=options)[0]
        return list(map(lambda tenant: tenant["tenantName"], response))

    def create_tenant(self, tenant, register_apps):
        resources = {"tenantName": self._tenant_name}
        options = {"registerApps": str(register_apps).lower()}
        return self.generic_method(
            CSdkOperationId.CREATE_TENANT_BIOS,
            resources=resources,
            options=options,
            data=tenant,
            timeout=0,  # no timeout
        )[0]

    def delete_tenant(self, tenant_name):
        resources = {"tenantName": tenant_name}
        return self.generic_method(CSdkOperationId.DELETE_TENANT_BIOS, resources=resources)[0]

    def list_app_tenants(self):
        resources = {"tenantName": self._tenant_name}
        return self.generic_method(CSdkOperationId.LIST_APP_TENANTS, resources=resources)[0]

    def insert_bulk(
        self, statement, option_atomic: Tuple[uuid.UUID, int] = None, tenant_name=None
    ):
        """Method to insert an event into a signal

        Args:
            statement (IsqlInsertRequest): an insert request statement
            option_atomic (Tuple[UUID, int]): option to specify atomic operation

        Returns:
            (InsertResponse) Object containing event id and timestamp

        Raises:
            ServiceErrors: (INVALID_ARGUMENT) when statement is not built
            ServiceError: (UNAUTHORIZED) When the logged in user does not have the
            permission to insert events to this signal
            ServiceError: (BAD_INPUT) when attributes do not match
        """
        num_events = len(statement.values)
        bulk_client = InsertBulkClient(statement, num_events)
        bulk_handler = IngestBulkHandler(bulk_client)
        options = (
            {
                "atomicOpId": str(option_atomic[0]),
                "atomicOpCount": str(option_atomic[1]),
            }
            if option_atomic
            else {}
        )
        resources = {}
        if tenant_name:
            resources["tenant"] = tenant_name
            options["delegate"] = "true"
        try:
            self._insert_bulk(statement, bulk_handler, num_events, resources, options)
            return InsertResponse(bulk_client.process_response())
        except ServiceError as err:
            return bulk_client.process_error_response(err)

    def _insert_bulk(self, statement, bulk_handler, num_events, resources, options):
        start_time = time.time()
        result = self._run(
            self._insert_bulk_core(
                CSdkOperationId.INSERT_BULK_PROTO,
                num_events,
                bulk_handler,
                resources,
                options,
            )
        )
        endpoint = result[2] or ""
        csdk_elapsed = result[4] or 0
        wrapper_elapsed = math.ceil((time.time() - start_time) * 1000 * 1000)
        stream = statement.signal
        try:
            result = self._handle_response(result[0], result[1], result[2], True)
            self._csdk.log(
                self._session_handle,
                self._session_id,
                int(start_time * 1000),
                csdk_elapsed,
                wrapper_elapsed,
                0,
                num_events,
                str(CSdkOperationId.INSERT_BULK_PROTO),
                "OK",
                str(num_events),
                endpoint,
                stream or "",
                0,
                0,
                0,
            )
            return result
        except ServiceError as err:
            self._csdk.log(
                self._session_handle,
                self._session_id,
                int(start_time * 1000),
                csdk_elapsed,
                wrapper_elapsed,
                0,
                0,
                str(CSdkOperationId.INSERT_BULK_PROTO),
                err.error_code.name,
                "message=" + err.message,
                endpoint,
                stream or "",
                0,
                0,
                0,
            )
            raise

    async def _insert_bulk_core(self, op_id, num_events, bulk_handler, resources, options):
        """Internal coroutine to execute a C-SDK ingest bulk operation."""
        (seqno, future) = self._create_future()
        self._pending_ingest_bulks[seqno] = bulk_handler
        bulk_handler.start_ingest_bulk(
            self._csdk,
            self._session_handle,
            self._session_id,
            seqno,
            op_id,
            num_events,
            resources,
            options,
        )
        result = await future
        del self._pending_ingest_bulks[seqno]
        return result

    def upsert(self, statement: ISqlUpsertCompleteRequest, tenant_name=None) -> None:
        """Method to insert or update context entries.
        Args:
            statement: ISqlUpsertCompleteRequest
            option_atomic: option to specify atomic operation

        Returns:
            (EmptyISqlResponse) Empty return
        Raises:
            ServiceError if there is a problem executing the upsert request.
        """
        resources = {"stream": statement.context}
        options = {}
        if tenant_name is not None:
            resources["tenant"] = tenant_name
            options["delegate"] = "true"
        self.generic_method(
            CSdkOperationId.CREATE_CONTEXT_ENTRY_BIOS,
            resources=resources,
            options=options,
            data=str(statement),
            num_upserts=len(statement.values),
        )
        return EmptyISqlResponse(ISqlResponseType.UPSERT)

    def update_context_entry(self, statement: ISqlUpdateContext) -> None:
        """Method to update context entry.
        Args:
            statement: ISqlUpdateContext

        Returns:
            (EmptyISqlResponse) Empty return
        Raises:
            ServiceError if there is a problem executing the update request.
        """
        self.simple_method(
            CSdkOperationId.UPDATE_CONTEXT_ENTRY_BIOS,
            tenant=self._tenant_name,
            stream=statement.name,
            data=str(statement),
        )
        return EmptyISqlResponse(ISqlResponseType.UPDATE_CONTEXT)

    def delete_context_entry(self, statement: ISqlDeleteContext) -> None:
        """Method to delete context entries.
        Args:
            statement: ISqlDeleteContext

        Returns:
            (EmptyISqlResponse) Empty return
        Raises:
            ServiceError if there is a problem executing the delete request.
        """
        self.simple_method(
            CSdkOperationId.DELETE_CONTEXT_ENTRY_BIOS,
            tenant=self._tenant_name,
            stream=statement.name,
            data=str(statement),
        )
        return EmptyISqlResponse(ISqlResponseType.DELETE_CONTEXT)

    def select_context_entry(self, statement: ISqlSelectFromContext) -> dict:
        """Method to select context entries.
        Args:
            statement: ISqlSelectFromContext

        Returns:
            (ContextRecords) object containing the selected records
        Raises:
            ServiceError if there is a problem executing the select request.
        """
        wrapper_start = time.time()
        resp = self.simple_method(
            CSdkOperationId.GET_CONTEXT_ENTRIES_BIOS,
            tenant=self._tenant_name,
            stream=statement.name,
            data=str(statement),
            skip_logging=True,
        )
        ret = ContextRecords(resp[0])
        num_records = len(ret.get_records())
        wrapper_elapsed = math.ceil((time.time() - wrapper_start) * 1000 * 1000)
        self._csdk.log(
            self._session_handle,
            self._session_id,
            resp[2],
            resp[3],
            wrapper_elapsed,
            num_records,
            0,
            str(CSdkOperationId.GET_CONTEXT_ENTRIES_BIOS),
            "OK",
            "",
            resp[1],
            statement.name,
            resp[4],
            resp[5],
            resp[6],
        )
        return ret

    def select_context_entry_ex(self, statement: ISqlSelectRequestMessageBuilderEx):
        """Method to select context record.
        Args:
            statement: SelectFromContextRequestEx

        Returns:
            (InsertResponse) contains event id and timestamp.
        Raises:
            ServiceError if there is a problem executing the select request.
        """
        wrapper_start = time.time()
        resources = {
            "stream": statement.query["context"],
            "tenantName": self._tenant_name,
            "eventId": str(uuid.uuid1(node=uuid.getnode())),
        }
        resp = self.generic_method(
            CSdkOperationId.SELECT_CONTEXT_ENTRIES,
            resources=resources,
            data=statement.query,
            skip_logging=True,
        )
        ret = SelectContextResponse(resp[0])
        num_records = len(ret.get_records())
        wrapper_elapsed = math.ceil((time.time() - wrapper_start) * 1000 * 1000)
        self._csdk.log(
            self._session_handle,
            self._session_id,
            resp[2],
            resp[3],
            wrapper_elapsed,
            num_records,
            0,
            str(CSdkOperationId.SELECT_CONTEXT_ENTRIES),
            "OK",
            "",
            resp[1],
            statement.query["context"],
            0,
            0,
            0,
        )
        return ret

    def insert(
        self, statement: ISqlInsertCompleteRequest, tenant_name: str = None
    ) -> InsertResponse:
        """Method to insert events.
        Args:
            statement (ISqlInsertCompleteRequest): ISqlInsertCompleteRequest
            tenant (str): Tenant name

        Returns:
            (InsertResponse) contains event id and timestamp.
        Raises:
            ServiceError if there is a problem executing the select request.
        """
        resources = {
            "stream": statement.signal,
            "eventId": str(uuid.uuid1(node=uuid.getnode())),
        }
        options = {}
        if tenant_name is not None:
            resources["tenant"] = tenant_name
            options["delegate"] = "true"
        resp = self.generic_method(
            CSdkOperationId.INSERT_PROTO,
            resources=resources,
            options=options,
            data=statement.req_str,
            raw_out=True,
        )[0]
        return InsertResponse([decode_insert_response_record(resp)])

    def single_select(self, statement, tenant_name: str = None):
        return self.multi_select([statement], tenant_name)

    def multi_select(self, statements, tenant_name: str = None):
        wrapper_start = time.time()
        check_list(
            statements,
            "statements",
            [ISqlSelectRequestMessage, ISqlSelectFromContext],
            False,
            True,
        )
        if isinstance(statements[0], ISqlSelectRequestMessage):
            check_list(statements, "statements", ISqlSelectRequestMessage, False, True)
            operation_id = CSdkOperationId.SELECT_PROTO
            request = data_pb2.SelectRequest()
            for statement in statements:
                request.queries.append(statement.request)
            resources = {}
            if tenant_name is not None:
                resources["tenant"] = tenant_name
            resp = self.generic_method(
                operation_id,
                resources=resources,
                data=request.SerializeToString(),
                raw_out=True,
                skip_logging=True,
            )

            if len(statements) == 1:
                stream = getattr(statements[0].request, "from")
            else:
                stream = ""

            ret = decode_select_response(resp[0])
            num_records = 0
            for select_resp in ret:
                for data_window in select_resp.get_data_windows():
                    num_records = num_records + len(data_window.records)
        elif isinstance(statements[0], ISqlSelectFromContext):
            check_list(statements, "statements", ISqlSelectFromContext, False, True)
            operation_id = CSdkOperationId.MULTI_GET_CONTEXT_ENTRIES_BIOS
            if len(statements) == 1:
                stream = statements[0].name
            else:
                stream = ""
            context_names = []
            queries = []
            for statement in statements:
                context_names.append(statement.name)
                queries.append(statement)
            request = MultiGetContextEntriesRequest(context_names, queries)
            resp = self.simple_method(
                operation_id,
                tenant=self._tenant_name,
                stream=stream,
                data=str(request),
                skip_logging=True,
            )
            ret = []
            num_records = 0
            for context_records_list in resp[0].get("entryLists"):
                next_list_object = ContextRecords(context_records_list)
                ret.append(next_list_object)
                num_records = num_records + len(next_list_object.get_records())
        else:
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT,
                (
                    f"Parameter 'statement' must be of type ISqlSelectRequestMessage or"
                    f"ISqlSelectFromContext, but {type(statements[0])} was specified",
                ),
            )

        wrapper_elapsed = math.ceil((time.time() - wrapper_start) * 1000 * 1000)
        num_qos_retry_considered = resp[4]
        num_qos_retry_sent = resp[5]
        num_qos_retry_response_used = resp[6]
        self._csdk.log(
            self._session_handle,
            self._session_id,
            resp[2],
            resp[3],
            wrapper_elapsed,
            num_records,
            0,
            str(operation_id),
            "OK",
            "",
            resp[1],
            stream,
            num_qos_retry_considered,
            num_qos_retry_sent,
            num_qos_retry_response_used,
        )
        return ret

    def create_export_destination(self, config):
        return self.generic_method(
            CSdkOperationId.CREATE_EXPORT_DESTINATION,
            data=config,
        )[0]

    def get_export_destination(self, destination_id):
        resources = {"storageName": destination_id}
        return self.generic_method(CSdkOperationId.GET_EXPORT_DESTINATION, resources=resources)[0]

    def update_export_destination(self, destination_id, config):
        resources = {"storageName": destination_id}
        return self.generic_method(
            CSdkOperationId.UPDATE_EXPORT_DESTINATION, resources=resources, data=config
        )[0]

    def delete_export_destination(self, destination_id):
        resources = {"storageName": destination_id}
        self.generic_method(CSdkOperationId.DELETE_EXPORT_DESTINATION, resources=resources)

    def start_data_export(self, storage_name):
        resources = {"storageName": storage_name}
        return self.generic_method(CSdkOperationId.DATA_EXPORT_START, resources=resources)[0]

    def stop_data_export(self, storage_name):
        resources = {"storageName": storage_name}
        return self.generic_method(CSdkOperationId.DATA_EXPORT_STOP, resources=resources)[0]

    def create_tenant_appendix(
        self,
        category: TenantAppendixCategory,
        content: Union[dict, str],
        entry_id: str = None,
    ) -> dict:
        tenant_appendix_spec = {
            "@type": category.name,
            "content": content,
        }
        if entry_id:
            tenant_appendix_spec["entryId"] = entry_id
        resources = {"category": category.name}
        return self.generic_method(
            CSdkOperationId.CREATE_TENANT_APPENDIX,
            data=tenant_appendix_spec,
            resources=resources,
        )[0]

    def get_tenant_appendix(self, category: TenantAppendixCategory, entry_id: str) -> dict:
        resources = {"category": category.name, "entryId": entry_id}
        return self.generic_method(CSdkOperationId.GET_TENANT_APPENDIX, resources=resources)[0]

    def update_tenant_appendix(
        self,
        category: TenantAppendixCategory,
        content: Union[dict, str],
        entry_id: str,
    ) -> dict:
        tenant_appendix_spec = {
            "@type": category.name,
            "content": content,
        }
        resources = {"category": category.name, "entryId": entry_id}
        return self.generic_method(
            CSdkOperationId.UPDATE_TENANT_APPENDIX,
            data=tenant_appendix_spec,
            resources=resources,
        )[0]

    def delete_tenant_appendix(self, category: TenantAppendixCategory, entry_id: str) -> dict:
        resources = {"category": category.name, "entryId": entry_id}
        self.generic_method(
            CSdkOperationId.DELETE_TENANT_APPENDIX,
            resources=resources,
        )

    def discover_import_subjects(self, import_source_id: str, timeout: int) -> dict:
        resources = {"importSourceId": import_source_id}
        options = {"timeout": str(timeout)} if timeout is not None else {}
        return self.generic_method(
            CSdkOperationId.DISCOVER_IMPORT_SUBJECTS,
            resources=resources,
            options=options,
        )[0]

    def create_user(self, user_config):
        return self.generic_method(CSdkOperationId.CREATE_USER, data=user_config)[0]

    def get_users(self, emails: List[str] = None) -> dict:
        options = {}
        if emails:
            options["email"] = ",".join(emails)
        return self.generic_method(CSdkOperationId.GET_USERS, options=options)[0]

    def modify_user(self, user_id: str, full_name: str, roles: List[str], status: str):
        user_config = {}
        if full_name:
            user_config["fullName"] = full_name
        if roles:
            user_config["roles"] = roles
        if status:
            user_config["status"] = status
        resources = {"userId": user_id}
        return self.generic_method(
            CSdkOperationId.MODIFY_USER, resources=resources, data=user_config
        )[0]

    def delete_user(self, email, user_id):
        options = {}
        if email:
            options["email"] = email
        if user_id:
            options["userId"] = str(user_id)
        self.generic_method(CSdkOperationId.DELETE_USER, options=options)

    def register_apps_service(
        self, tenant_name: str, hosts: List[str], control_port: int, webhook_port: int
    ):
        apps_info = {
            "tenantName": tenant_name,
            "hosts": hosts,
            "controlPort": control_port,
            "webhookPort": webhook_port,
        }
        return self.generic_method(CSdkOperationId.REGISTER_APPS_SERVICE, data=apps_info)[0]

    def get_apps_info(self, tenant_name: str) -> dict:
        return self.simple_method(CSdkOperationId.GET_APPS_INFO, tenant=tenant_name)[0]

    def deregister_apps_service(self, tenant_name: str) -> dict:
        return self.simple_method(CSdkOperationId.DEREGISTER_APPS_SERVICE, tenant=tenant_name)[0]

    def change_password(self, current_password: str, new_password: str, email: str):
        request = {
            "currentPassword": current_password,
            "newPassword": new_password,
        }
        if email is not None:
            request["email"] = email
        self.simple_method(CSdkOperationId.RESET_PASSWORD, data=request)

    def get_property(self, key):
        resources = {"property": key}
        return (
            self.generic_method(
                CSdkOperationId.GET_PROPERTY,
                resources=resources,
                raw_out=True,
            )[0]
            or ""
        )

    def set_property(self, key, value):
        resources = {"property": key}
        self.generic_method(
            CSdkOperationId.SET_PROPERTY,
            resources=resources,
            data=value,
        )

    def maintain_keyspaces(self, request: dict):
        return self.generic_method(CSdkOperationId.MAINTAIN_KEYSPACES, data=request)[0]

    def maintain_tables(self, request: dict):
        return self.generic_method(CSdkOperationId.MAINTAIN_TABLES, data=request)[0]

    def maintain_context(self, request: dict):
        return self.generic_method(CSdkOperationId.MAINTAIN_CONTEXT, data=request)[0]

    def get_report_configs(self, report_ids: List[str] = None, detail=None) -> dict:
        options = {}
        if report_ids:
            options["ids"] = ",".join(report_ids)
        if detail is not None:
            options["detail"] = "true" if detail else "false"
        return self.generic_method(CSdkOperationId.GET_REPORT_CONFIGS, options=options)[0]

    def put_report_config(self, report: dict) -> None:
        resources = {
            "tenantName": self._tenant_name,
            "reportId": report["reportId"],
        }
        self.generic_method(CSdkOperationId.PUT_REPORT_CONFIG, resources=resources, data=report)

    def delete_report(self, report_id: str) -> None:
        resources = {
            "tenantName": self._tenant_name,
            "reportId": report_id,
        }
        self.generic_method(CSdkOperationId.DELETE_REPORT, resources=resources)

    def get_insight_configs(self, insight_name: str) -> dict:
        resources = {
            "tenantName": self._tenant_name,
            "insightName": insight_name,
        }
        return self.generic_method(CSdkOperationId.GET_INSIGHT_CONFIGS, resources=resources)[0]

    def put_insight_configs(self, insight_name: str, insight_configs: Any):
        resources = {"tenantName": self._tenant_name, "insightName": insight_name}
        self.generic_method(
            CSdkOperationId.PUT_INSIGHT_CONFIGS, resources=resources, data=insight_configs
        )

    def delete_insight_configs(self, insight_name: str):
        resources = {"tenantName": self._tenant_name, "insightName": insight_name}
        self.generic_method(CSdkOperationId.DELETE_INSIGHT_CONFIGS, resources=resources)

    def feature_status(self, feature_status_request: dict) -> dict:
        resources = {
            "tenantName": self._tenant_name,
        }
        return self.generic_method(
            CSdkOperationId.FEATURE_STATUS, resources=resources, data=feature_status_request
        )[0]

    def feature_refresh(self, feature_refresh_request: dict) -> None:
        resources = {
            "tenantName": self._tenant_name,
        }
        self.generic_method(
            CSdkOperationId.FEATURE_REFRESH, resources=resources, data=feature_refresh_request
        )

    def list_endpoints(self):
        return self.simple_method(CSdkOperationId.LIST_ENDPOINTS)[0]

    def delete_endpoint(self, endpoint):
        self.simple_method(
            CSdkOperationId.UPDATE_ENDPOINTS,
            data=str(UpdateEndpointsRequest(Operation.REMOVE, endpoint)),
        )

    def register_for_service(self, domain, email, service_type):
        request = {"domain": domain, "email": email, "serviceType": service_type}
        return self.simple_method(CSdkOperationId.REGISTER_FOR_SERVICE, data=request)[0]

    def stores_query(self, domain, tenant_name, session_id, client_id, product_id, queries):
        request = {
            "domain": domain,
            "tenantName": tenant_name,
            "sessionId": session_id,
            "clientId": client_id,
            "productId": product_id,
            "queries": queries,
        }
        return self.simple_method(CSdkOperationId.STORES_QUERY, data=request)[0]

    def send_log_message(self, message):
        self.simple_method(CSdkOperationId.TEST_LOG, data={"message": message})

    @classmethod
    def _add_atomic_option(cls, option_atomic: Tuple[str, int], options: dict) -> dict:
        """Utility to add atomic operation parameters to an options dictionary.

        The method writes to the specified dictionary in place.

        Args:
            option_atomic (Tuple[str, int]): Atomic operation specification. The value can be None.
            options (dict): Options dictionary to be written.

        Returns:
            dict: Updated options
        """
        if option_atomic:
            options["atomicOpId"] = str(option_atomic[0])
            options["atomicOpCount"] = str(option_atomic[1])
        return options
