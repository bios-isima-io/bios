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

import base64
import json
import uuid
from typing import Any, List, Union
from urllib.parse import urlparse

from ._csdk import BiosCSdkSession
from ._utils import check_not_empty_string, check_str_list, check_type
from .errors import ErrorCode, ServiceError
from .models import (
    AppType,
    ISqlRequestMessage,
    ISqlRequestType,
    ISqlResponse,
    NodeType,
    TenantAppendixCategory,
    User,
)


class Delegate:
    """Temporary client to act for another tenant.

    Only App User has permission to make a request using this class

    Args:
        client (Client): The original biOS client
        tenant_name (str): Tenant name to be deleted for
    """

    def __init__(self, client: "Client", tenant_name: str):
        check_not_empty_string(tenant_name, "tenant_name")
        self._tenant_name = tenant_name
        self._client = client

    def get_tenant_name(self):
        return self._tenant_name

    def execute(self, statement):
        return self._client.execute_core(statement, False, tenant_name=self._tenant_name)

    def create_signal(self, signal):
        """Method to create a  signal with configuration as passed in signal param.

        Args:
            signal (str or dict): the signal configuration can be json or dictionary

        Returns:
            signal configuration as a dictionary with signal id added

        Raises:
            ServiceError: (INVALID_ARGUMENT) when signal is not a string nor a dict
            ServiceError: (UNAUTHORIZED) When the logged in user does not have the
            permission to create this signal
        """
        return self._client.session.create_signal(signal, tenant_name=self._tenant_name)

    def get_signals(
        self,
        names: List[str] = None,
        detail: bool = False,
        include_internal: bool = False,
        inferred_tags: bool = False,
    ) -> List[dict]:
        """Method to return all the signals the user has access to.

        Args:
            names (list[str]): (Optional) Names to filter the signals. List of names of signals
            detail (bool): (Optional, Default=False) Returns detailed signal information
            include_internal (bool): (Optional, Default=False) Include internals of the signal
            inferred_tags(bool): (Optional, Default=False) Include inferred tags in the reply

        Returns:
            List of signals

        Raises: ServiceError if the arguments are bad or there is some exception in the stack
        """
        return self._client.session.get_signals(
            names=names,
            detail=detail,
            include_internal=include_internal,
            inferred_tags=inferred_tags,
            tenant_name=self._tenant_name,
        )

    def get_signal(self, signal_name):
        """Method to return signal with name signal_name.

        Args:
            signal_name (str): name of the signal to be retrieved
            inferred_tags(bool): (Optional, Default=False) Include inferred tags in the reply

        Returns:
            signal configuration as a dictionary

        Raises:
            ServiceError: (INVALID_ARGUMENT) when name is not a string
            ServiceError: (UNAUTHORIZED) When the logged in user does not have the
            permission to get this signal
        """
        check_type(signal_name, "signal_name", str)
        return self._client.session.get_signals(
            [signal_name], detail=True, include_internal=True, tenant_name=self._tenant_name
        )[0]

    def update_signal(self, signal_name, signal_config):
        """Method to update a signal with name as passed in signal_name and
        configuration as passed in signal_config.

        Args:
            signal_name (str): the name of the signal to be modified
            signal_config (str or dict): the signal configuration can be json or dictionary

        Returns:
            signal configuration as a dictionary with signal id added

        Raises:
            ServiceError: (INVALID_ARGUMENT) when signal is not a string nor a dict
            ServiceError: (UNAUTHORIZED) When the logged in user does not have the
            permission to update this signal
        """
        return self._client.session.update_signal(
            signal_name, signal_config, tenant_name=self._tenant_name
        )

    def delete_signal(self, signal_name):
        """Method to delete the signal specified by signal_name.

        Args:
            signal_name (str): name of the signal to be deleted

        Raises:
            ServiceError: (INVALID_ARGUMENT) when signal_name is not a string
            ServiceError: (UNAUTHORIZED) When the logged in user does not have the
            permission to delete this signal
        """
        return self._client.session.delete_signal(signal_name, tenant_name=self._tenant_name)

    def create_context(self, context: Union[dict, str]):
        """Method to create a  context with configuration as passed in context param.

        Args:
            context (str or dict): the context configuration can be json or dictionary

        Returns:
            context configuration as a dictionary with context id added

        Raises:
            ServiceError: (INVALID_ARGUMENT) when context is not a string nor a dict
            ServiceError: (UNAUTHORIZED) When the logged in user does not have the
            permission to create this context
        """
        return self._client.session.create_context(context, tenant_name=self._tenant_name)

    def get_contexts(
        self,
        names: List[str] = None,
        detail: bool = False,
        include_internal: bool = False,
        inferred_tags=False,
    ):
        """Method to return all the contexts the user has access to.

        Args:
            names (list[str]): (Optional) Names to filter the contexts. List of names of contexts
            detail (bool): (Optional, Default=False) Returns detailed context information
            include_internal (bool): (Optional, Default=False) Include internals of the signal
            inferred_tags(bool): (Optional, Default=False) Include inferred tags in the reply

        Returns:
            List of contexts

        Raises: ServiceError if the arguments are bad or there is some exception in the stack

        """
        return self._client.session.get_contexts(
            names=names,
            detail=detail,
            include_internal=include_internal,
            inferred_tags=inferred_tags,
            tenant_name=self._tenant_name,
        )

    def get_context(self, context_name: str, inferred_tags: bool = False):
        """Method to return context with name context_name.

        Args:
            context_name (str): name of the context to be retrieved
            inferred_tags(bool): (Optional, Default=False) Include inferred tags in the reply

        Returns:
            context configuration as a dictionary

        Raises:
            ServiceError: (INVALID_ARGUMENT) when name is not a string
            ServiceError: (UNAUTHORIZED) When the logged in user does not have the
            permission to get this context
        """
        check_type(context_name, "context_name", str)
        return self._client.session.get_contexts(
            names=[context_name],
            detail=True,
            inferred_tags=inferred_tags,
            tenant_name=self._tenant_name,
        )[0]

    def update_context(self, context_name, context_config):
        """Method to update a context with name as passed in context_name and
        configuration as passed in context_config.

        Args:
            context_name (str): the name of the context to be modified
            context_config (str or dict): the context configuration can be json or dictionary

        Returns:
            context configuration as a dictionary with context id added

        Raises:
            ServiceError: (INVALID_ARGUMENT) when context is not a string nor a dict
            ServiceError: (UNAUTHORIZED) When the logged in user does not have the
            permission to update this context
        """
        return self._client.session.update_context(
            context_name, context_config, tenant_name=self._tenant_name
        )

    def delete_context(self, context_name):
        """Method to delete the context specified by context_name.

        Args:
            context_name (str): name of the context to be deleted

        Raises:
            ServiceError: (INVALID_ARGUMENT) when context_name is not a string
            ServiceError: (UNAUTHORIZED) When the logged in user does not have the
            permission to delete this context
        """
        self._client.session.delete_context(context_name, tenant_name=self._tenant_name)


class Client:
    """biOS client class that provides access to biOS services.

    The class covers all service planes. In order to start a session to a service plane,
    it is required to sign-in. Three login methods are provided for signing-in.

    Use method :meth:`bios.login()` to generate a code:`Client` object with signed-in state.

    A certification authority (CA) file can be specified by optional argument :code:`cafile`.
    This also can be specified by environment variable :code:`SSL_CERT_FILE`.

    Args:
        host (str): Server host name
        port (int): Server port number
        ssl_enabled (bool): Whether if the server connection uses secured connection
        cafile (str): (Optional) Path to a file of concatenated CA certificates in PEM format.
    """

    _OP_TIMEOUT = 480000
    _max_ingests = 100000

    def __init__(
        self,
        host,
        port,
        ssl_enabled=True,
        cafile=None,
        timeout=None,
    ):
        self.session = None  # To ensure the existence of the property
        self.session = BiosCSdkSession()
        # pylint: disable=duplicate-code
        self.session.start_session(
            host,
            port,
            ssl_enabled,
            "",
            timeout or self._OP_TIMEOUT,
            cafile,
        )
        self.started = False
        self.is_active = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._close(True)

    def __del__(self):
        # A case that this method is called while the session is open is likely to happen
        # during process shutdown (session is left open until the process ends). In such a case,
        # the session may need to be closed brutally since event loop may not work as expected.
        self._close(False)

    def _login(self, email: str, password: str, app_name: str, app_type: AppType) -> "Client":
        """This method logs in a user

        After calling this method, the client is able to use data plane operation methods.

        Once this method is called, signing-in to another service plane is prohibited.
        Create another client in order to access different service plane.

        Args:
            email (str): email of the user
            password (str): password

        Returns: Client: Self.

        Raises:
            ServiceError: (UNAUTHORIZED) When specified authentication credentials are invalid.
            ServiceError: (CLIENT_ALREADY_STARTED) When the client has already signed in.
        """
        if self.started:
            raise ServiceError(
                ErrorCode.CLIENT_ALREADY_STARTED,
                "Client has been started already.",
            )

        self.session.login(email, password, app_name, app_type)
        self.started = True
        self.is_active = True
        return self

    def get_tenant_name(self):
        """Gets tenantname"""
        return self.session.get_tenant_name()

    def login(self, email: str, password: str, app_name: str, app_type: AppType) -> "Client":
        return self._login(email, password, app_name, app_type)

    def close(self):
        """Method to close the client.

        The client is voided after calling this method and cannot use anymore.
        """
        self._close(True)

    def _close(self, shutdown_gracefully):
        self.is_active = False
        if self.session:
            self.session.close(shutdown_gracefully)
        self.session = None

    def get_signals(
        self,
        names: List[str] = None,
        detail: bool = False,
        include_internal: bool = False,
        inferred_tags: bool = False,
    ) -> List[dict]:
        """Method to return all the signals the user has access to.

        Args:
            names (list[str]): (Optional) Names to filter the signals. List of names of signals
            detail (bool): (Optional, Default=False) Returns detailed signal information
            include_internal (bool): (Optional, Default=False) Include internals of the signal
            inferred_tags(bool): (Optional, Default=False) Include inferred tags in the reply

        Returns:
            List of signals

        Raises: ServiceError if the arguments are bad or there is some exception in the stack
        """
        return self.session.get_signals(names, detail, include_internal, inferred_tags)

    def get_signal(self, signal_name: str, inferred_tags: bool = False) -> dict:
        """Method to return signal with name signal_name.

        Args:
            signal_name (str): name of the signal to be retrieved
            inferred_tags(bool): (Optional, Default=False) Include inferred tags in the reply

        Returns:
            signal configuration as a dictionary

        Raises:
            ServiceError: (INVALID_ARGUMENT) when name is not a string
            ServiceError: (UNAUTHORIZED) When the logged in user does not have the
            permission to get this signal
        """
        check_type(signal_name, "signal_name", str)
        return self.get_signals(
            names=[signal_name],
            detail=True,
            include_internal=True,
            inferred_tags=inferred_tags,
        )[0]

    def create_signal(self, signal: Union[dict, str]) -> dict:
        """Method to create a  signal with configuration as passed in signal param.

        Args:
            signal (str or dict): the signal configuration can be json or dictionary

        Returns:
            signal configuration as a dictionary with signal id added

        Raises:
            ServiceError: (INVALID_ARGUMENT) when signal is not a string nor a dict
            ServiceError: (UNAUTHORIZED) When the logged in user does not have the
            permission to create this signal
        """
        return self.session.create_signal(signal)

    def update_signal(self, signal_name: str, signal_config: dict) -> dict:
        """Method to update a signal with name as passed in signal_name and
        configuration as passed in signal_config.

        Args:
            signal_name (str): the name of the signal to be modified
            signal_config (str or dict): the signal configuration can be json or dictionary

        Returns:
            signal configuration as a dictionary with signal id added

        Raises:
            ServiceError: (INVALID_ARGUMENT) when signal is not a string nor a dict
            ServiceError: (UNAUTHORIZED) When the logged in user does not have the
            permission to update this signal
        """
        return self.session.update_signal(signal_name, signal_config)

    def delete_signal(self, signal_name: str):
        """Method to delete the signal specified by signal_name.

        Args:
            signal_name (str): name of the signal to be deleted

        Raises:
            ServiceError: (INVALID_ARGUMENT) when signal_name is not a string
            ServiceError: (UNAUTHORIZED) When the logged in user does not have the
            permission to delete this signal
        """
        self.session.delete_signal(signal_name)

    def get_contexts(
        self,
        names: List[str] = None,
        detail: bool = False,
        include_internal: bool = False,
        inferred_tags=False,
    ):
        """Method to return all the contexts the user has access to.

        Args:
            names (list[str]): (Optional) Names to filter the contexts. List of names of contexts
            detail (bool): (Optional, Default=False) Returns detailed context information
            include_internal (bool): (Optional, Default=False) Include internals of the signal
            inferred_tags(bool): (Optional, Default=False) Include inferred tags in the reply

        Returns:
            List of contexts

        Raises: ServiceError if the arguments are bad or there is some exception in the stack

        """
        return self.session.get_contexts(names, detail, include_internal, inferred_tags)

    def get_context(self, context_name: str, inferred_tags: bool = False):
        """Method to return context with name context_name.

        Args:
            context_name (str): name of the context to be retrieved
            inferred_tags(bool): (Optional, Default=False) Include inferred tags in the reply

        Returns:
            context configuration as a dictionary

        Raises:
            ServiceError: (INVALID_ARGUMENT) when name is not a string
            ServiceError: (UNAUTHORIZED) When the logged in user does not have the
            permission to get this context
        """
        check_type(context_name, "context_name", str)
        return self.get_contexts(names=[context_name], detail=True, inferred_tags=inferred_tags)[0]

    def create_context(self, context: Union[dict, str]):
        """Method to create a  context with configuration as passed in context param.

        Args:
            context (str or dict): the context configuration can be json or dictionary

        Returns:
            context configuration as a dictionary with context id added

        Raises:
            ServiceError: (INVALID_ARGUMENT) when context is not a string nor a dict
            ServiceError: (UNAUTHORIZED) When the logged in user does not have the
            permission to create this context
        """
        return self.session.create_context(context)

    @staticmethod
    def _capitalize(value: str):
        """returns capitalized version of the string"""

        return value[0].upper() + value[1:]

    def update_context(self, context_name, context_config):
        """Method to update a context with name as passed in context_name and
        configuration as passed in context_config.

        Args:
            context_name (str): the name of the context to be modified
            context_config (str or dict): the context configuration can be json or dictionary

        Returns:
            context configuration as a dictionary with context id added

        Raises:
            ServiceError: (INVALID_ARGUMENT) when context is not a string nor a dict
            ServiceError: (UNAUTHORIZED) When the logged in user does not have the
            permission to update this context
        """
        return self.session.update_context(context_name, context_config)

    def delete_context(self, context_name):
        """Method to delete the context specified by context_name.

        Args:
            context_name (str): name of the context to be deleted

        Raises:
            ServiceError: (INVALID_ARGUMENT) when context_name is not a string
            ServiceError: (UNAUTHORIZED) When the logged in user does not have the
            permission to delete this context
        """
        self.session.delete_context(context_name)

    def get_all_context_synopses(self, current_time: int = None):
        """Gets all context synopses in the tenant.

        Args:
            current_time (int): Optional current time to retrieve synopses

        Returns:
            Synopses of all contexts in the tenant
        """
        if current_time is not None:
            check_type(current_time, "current_time", int)
        return self.session.get_all_context_synopses(current_time)

    def get_context_synopsis(self, context_name: str, current_time: int = None):
        """Gets a context synopsis. The result includes attribute synopses

        Args:
            context_name (str): Context name
            current_time (int): Optional current time to retrieve synopses

        Returns:
            Synopsis of the context
        """
        check_type(context_name, "context_name", str)
        if current_time is not None:
            check_type(current_time, "current_time", int)
        return self.session.get_context_synopsis(context_name, current_time)

    def list_tenants(self, names: List[str] = None) -> List[dict]:
        """Method to return all the tenants the user has access to.

        Args:
            names (list[str]): (Optional) Names to filter the tenants. List of names of tenants

        Returns:
            List of tenants

        Raises: ServiceError if the arguments are bad or there is some exception in the stack

        """
        if names is not None:
            check_type(names, "names", [str, list])
        return self.session.list_tenants(names)

    def get_tenant(
        self,
        tenant_name: str = None,
        detail: bool = False,
        include_internal: bool = False,
        inferred_tags: bool = False,
    ) -> dict:
        """Method to return tenant with name tenant_name.

        Args:
            tenant_name (str): (Optional, Default=tenant of the user)
                Name of the tenant to be retrieved
            detail (bool): (Optional, Default=False) Returns detailed tenant information
            include_internal (bool): (Optional, Default=False) Include internals of the tenant
            inferred_tags(bool): (Optional, Default=False) Include inferred tags in the reply

        Returns:
            tenant configuration as a dictionary

        Raises:
            ServiceError: (INVALID_ARGUMENT) when name is not a string
            ServiceError: (UNAUTHORIZED) When the logged in user does not have the
            permission to get this tenant
        """
        if tenant_name is not None:
            check_type(tenant_name, "tenant_name", str)
        if detail is not None:
            check_type(detail, "detail", bool)
        if include_internal is not None:
            check_type(include_internal, "include_internal", bool)
        if inferred_tags is not None:
            check_type(inferred_tags, "inferred_tags", bool)
        return self.session.get_tenant(tenant_name, detail, include_internal, inferred_tags)

    def create_tenant(self, tenant: dict, register_apps: bool = False) -> dict:
        """Method to create a  tenant with configuration as passed in tenant param.

        Args:
            tenant (dict): The tenant configuration can be json or dictionary
            register_apps (bool = False): The method registers biOS apps service if true

        Raises:
            ServiceError: (INVALID_ARGUMENT) when tenant is not a string nor a dict
            ServiceError: (UNAUTHORIZED) When the logged in user does not have the
            permission to create this tenant
        """
        check_type(tenant, "tenant", [str, dict])
        check_type(register_apps, "register_apps", bool)
        return self.session.create_tenant(tenant, register_apps)

    def delete_tenant(self, tenant_name: str):
        """Method to delete the tenant specified by tenant_name.

        Args:
            tenant_name (str): name of the tenant to be deleted

        Returns:
            Nothing

        Raises:
            ServiceError: (INVALID_ARGUMENT) when tenant_name is not a string
            ServiceError: (UNAUTHORIZED) When the logged in user does not have the
            permission to delete this tenant
        """
        check_type(tenant_name, "tenant_name", str)
        return self.session.delete_tenant(tenant_name)

    def list_app_tenants(self) -> List[str]:
        """Method to return all the tenants the user has access to.

        Returns:
            List of app tenants

        Raises: ServiceError if the arguments are bad or there is some exception in the stack

        """
        return self.session.list_app_tenants()

    def execute(self, statement: ISqlRequestMessage) -> ISqlResponse:
        """Method to execute an isql statement

        Args:
            statement (ISqlRequest): an isql request statement
            rollback_on_failure (bool): roll back data changes on failure
                                        (optional, default is False)
        Returns:
            depending on if the request is select or insert.

        Raises:
            ServiceError: (INVALID_ARGUMENT) when statement is not built
            ServiceError: (UNAUTHORIZED) When the logged in user does not have the
            permission to use this signal
            ServiceError: (BAD_INPUT) when attributes do not match
        """
        return self.execute_core(statement, False)

    def execute_core(
        self, statement: ISqlRequestMessage, rollback_on_failure: bool, tenant_name: str = None
    ) -> ISqlResponse:
        if not isinstance(statement, ISqlRequestMessage):
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT,
                "Statement must be an instance of ISqlRequestMessage",
            )
        check_type(rollback_on_failure, "rollback_on_failure", bool)
        option_atomic = (uuid.uuid4(), 1) if rollback_on_failure else None
        statement_type = statement.get_type()
        if rollback_on_failure and statement_type not in {
            ISqlRequestType.INSERT,
            ISqlRequestType.INSERT_BULK,
        }:
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT,
                "The rollback_on_failure option cannot be used for a statement to read data",
            )

        if statement_type == ISqlRequestType.INSERT:
            return self.session.insert(statement, tenant_name=tenant_name)
        if statement_type == ISqlRequestType.INSERT_BULK:
            return self.session.insert_bulk(statement, option_atomic, tenant_name=tenant_name)
        if statement_type == ISqlRequestType.SELECT:
            return self.session.single_select(statement, tenant_name=tenant_name)[0]
        if statement_type == ISqlRequestType.UPSERT:
            return self.session.upsert(statement, tenant_name=tenant_name)
        if statement_type == ISqlRequestType.UPDATE_CONTEXT:
            return self.session.update_context_entry(statement)
        if statement_type == ISqlRequestType.DELETE_CONTEXT:
            return self.session.delete_context_entry(statement)
        if statement_type == ISqlRequestType.SELECT_CONTEXT:
            return self.session.select_context_entry(statement)
        if statement_type == ISqlRequestType.SELECT_CONTEXT_EX:
            return self.session.select_context_entry_ex(statement)
        if statement_type == ISqlRequestType.ATOMIC_MUTATION:
            return self._mutate_atomically(statement)
        raise ServiceError(
            ErrorCode.INVALID_ARGUMENT,
            f"Statement of type {type(statement)} is not supported",
        )

    def _mutate_atomically(self, statement):
        if len(statement.statements) == 0:
            raise ServiceError(ErrorCode.BAD_INPUT, "Empty child statements")
        if len(statement.statements) > 1:
            raise ServiceError(
                ErrorCode.NOT_IMPLEMENTED,
                "Coming soon: Multiple statements to rollback on failure",
            )
        child_statement = statement.statements[0]
        statement_type = child_statement.get_type()
        if statement_type in {ISqlRequestType.INSERT, ISqlRequestType.INSERT_BULK}:
            return self.execute_core(child_statement, True)
        if statement_type == ISqlRequestType.ATOMIC_MUTATION:
            raise ServiceError(ErrorCode.BAD_INPUT, "rollback_on_failure must not be nested")
        raise ServiceError(
            ErrorCode.INVALID_REQUEST,
            f"Rolling back option is not supported for statement type {statement_type}",
        )

    def multi_execute(self, *statements):
        """Method to execute multiple iSQL statements

        Args:
            statements (ISqlSelectRequestMessage): iSQL statements

        Returns:
            list of objects: responses
        """
        return self.session.multi_select(statements)

    def create_export_destination(self, config: dict):
        """Creates an export destination

        Args:
          config (dict): Export destination config

        Returns:
          Created destination configuration
        """
        check_type(config, "config", [str, dict])
        return self.session.create_export_destination(config)

    def get_export_destination(self, export_destination_id: str) -> dict:
        """Retrieves an export destination

        Args:
          export_destination_id (str): Identifier for export destination

        Returns:
            Export Config
        """
        check_type(export_destination_id, "export_destination_id", str)
        return self.session.get_export_destination(export_destination_id)

    def update_export_destination(self, export_destination_id: str, config: dict) -> dict:
        """Updates a export destination

        Args:
          export_destination_id (str): Identifier for export destination
          config (dict): Export destination config

        Returns:
            Export Config
        """
        check_type(export_destination_id, "export_destination_id", str)
        check_type(config, "config", [str, dict])
        return self.session.update_export_destination(export_destination_id, config)

    def delete_export_destination(self, export_destination_id: str) -> dict:
        """Deletes a export destination

        Args:
          export_destination_id (str): Identifier for export destination

        Returns:
            Export Config
        """
        check_type(export_destination_id, "export_destination_id", str)
        return self.session.delete_export_destination(export_destination_id)

    def start_data_export(self, storage_name: str):
        """Method to start data export for the tenant

        Args:
          storage_name  (str): Identifier that identifies external storage

        Returns:
          Nothing
        """
        check_type(storage_name, "storage_name", str)
        return self.session.start_data_export(storage_name)

    def stop_data_export(self, storage_name: str):
        """Method to stop data export

        Args:
          storage_name (str): Identifier for external storage

        Returns:
          Nothing
        """
        check_type(storage_name, "storage_name", str)
        return self.session.stop_data_export(storage_name)

    def create_import_source(self, import_source_config: Union[dict, str]) -> dict:
        """Creates an import source configuration entry.

        Args:
            import_source_config (dict or str): Import source configuration

        Returns:
            Created import source configuration with property importSourceId
            generated by the server
        """
        check_type(import_source_config, "import_source_config", [dict, str])
        if isinstance(import_source_config, str):
            import_source_config = json.loads(import_source_config)
        entry_id = import_source_config.get("importSourceId")
        return self.session.create_tenant_appendix(
            TenantAppendixCategory.IMPORT_SOURCES,
            import_source_config,
            entry_id=entry_id,
        ).get("content")

    def get_import_source(self, import_source_id: str) -> dict:
        """Gets an import source configuration entry.

        Args:
            import_source_id (str): Import source configuration ID

        Returns:
            Specified import source configuration
        """
        check_type(import_source_id, "import_source_id", str)
        return self.session.get_tenant_appendix(
            TenantAppendixCategory.IMPORT_SOURCES, import_source_id
        ).get("content")

    def update_import_source(
        self,
        import_source_id: str,
        import_source_config: Union[dict, str],
    ) -> dict:
        """Updates an import source configuration entry.

        Args:
            import_source_id (str): Import source configuration ID
            import_source_config (dict or str): New import source configuration

        Returns:
            Updated import source configuration
        """
        check_type(import_source_id, "import_source_id", str)
        check_type(import_source_config, "import_source_config", [dict, str])
        if isinstance(import_source_config, str):
            import_source_config = json.loads(import_source_config)
        return self.session.update_tenant_appendix(
            TenantAppendixCategory.IMPORT_SOURCES,
            import_source_config,
            import_source_id,
        ).get("content")

    def delete_import_source(self, import_source_id: str):
        """Deletes an import source configuration entry.

        Args:
            import_source_id (str): Import source configuration ID
        """
        check_type(import_source_id, "import_source_id", str)
        self.session.delete_tenant_appendix(
            TenantAppendixCategory.IMPORT_SOURCES, import_source_id
        )

    def create_import_destination(self, import_destination_config: Union[dict, str]) -> dict:
        """Creates an import destination configuration entry.

        Args:
            import_destination_config (dict or str): Import destination configuration

        Returns:
            Created import destination configuration with property importDestinationId
            generated by the server
        """
        check_type(
            import_destination_config,
            "import_destination_config",
            [dict, str],
        )
        if isinstance(import_destination_config, str):
            import_destination_config = json.loads(import_destination_config)
        return self.session.create_tenant_appendix(
            TenantAppendixCategory.IMPORT_DESTINATIONS,
            import_destination_config,
            entry_id=import_destination_config.get("importDestinationId"),
        ).get("content")

    def get_import_destination(self, import_destination_id: str) -> dict:
        """Gets an import destination configuration entry.

        Args:
            import_destination_id (str): Import destination configuration ID

        Returns:
            Specified import destination configuration
        """
        check_type(import_destination_id, "import_destination_id", str)
        return self.session.get_tenant_appendix(
            TenantAppendixCategory.IMPORT_DESTINATIONS,
            import_destination_id,
        ).get("content")

    def update_import_destination(
        self,
        import_destination_id: str,
        import_destination_config: Union[dict, str],
    ) -> dict:
        """Updates an import destination configuration entry.

        Args:
            import_destination_id (str): Import destination configuration ID
            import_destination_config (dict or str): New import destination configuration

        Returns:
            Updated import destination configuration
        """
        check_type(import_destination_id, "import_destination_id", str)
        check_type(
            import_destination_config,
            "import_destination_config",
            [dict, str],
        )
        if isinstance(import_destination_config, str):
            import_destination_config = json.loads(import_destination_config)
        return self.session.update_tenant_appendix(
            TenantAppendixCategory.IMPORT_DESTINATIONS,
            import_destination_config,
            import_destination_id,
        ).get("content")

    def delete_import_destination(self, import_destination_id: str):
        """Deletes an import destination configuration entry.

        Args:
            import_destination_id (str): Import destination configuration ID
        """
        check_type(import_destination_id, "import_destination_id", str)
        self.session.delete_tenant_appendix(
            TenantAppendixCategory.IMPORT_DESTINATIONS,
            import_destination_id,
        )

    def create_import_flow_spec(self, import_flow_spec: Union[dict, str]) -> dict:
        """Creates an import flow specification entry.

        Args:
            import_flow_spec (dict or str): Import flow specification

        Returns:
            Created import flow specification with property importFlowId generated by the server
        """
        check_type(import_flow_spec, "import_flow_spec", [dict, str])
        if isinstance(import_flow_spec, str):
            import_flow_spec = json.loads(import_flow_spec)
        return self.session.create_tenant_appendix(
            TenantAppendixCategory.IMPORT_FLOW_SPECS,
            import_flow_spec,
            entry_id=import_flow_spec.get("importFlowId"),
        ).get("content")

    def get_import_flow_spec(self, import_flow_id: str) -> dict:
        """Gets an import flow specification entry.

        Args:
            import_flow_id (str): Import flow specification ID

        Returns:
            Specified import flow specification
        """
        check_type(import_flow_id, "import_flow_id", str)
        return self.session.get_tenant_appendix(
            TenantAppendixCategory.IMPORT_FLOW_SPECS, import_flow_id
        ).get("content")

    def update_import_flow_spec(
        self, import_flow_id: str, import_flow_spec: Union[dict, str]
    ) -> dict:
        """Updates an import flow specification entry.

        Args:
            import_flow_id (str): Import flow specification ID
            import_flow_spec (dict or str): New import flow specification

        Returns:
            Updated import flow specification
        """
        check_type(import_flow_id, "import_flow_id", str)
        check_type(import_flow_spec, "import_flow_spec", [dict, str])
        if isinstance(import_flow_spec, str):
            import_flow_spec = json.loads(import_flow_spec)
        return self.session.update_tenant_appendix(
            TenantAppendixCategory.IMPORT_FLOW_SPECS,
            import_flow_spec,
            import_flow_id,
        ).get("content")

    def delete_import_flow_spec(self, import_flow_id: str):
        """Deletes an import flow specification entry.

        Args:
            import_flow_id (str): Import flow specification ID
        """
        check_type(import_flow_id, "import_flow_id", str)
        self.session.delete_tenant_appendix(
            TenantAppendixCategory.IMPORT_FLOW_SPECS, import_flow_id
        )

    def create_import_data_processor(
        self, processor_name: str, processor_code: str = None, raw_config: dict = None
    ):
        """Creates an import data processor entry.

        Args:
            processor_name (str): Import data processor name
            processor_code (str): Unencoded import data processor code (default=None)
            raw_config (dict): Import data mapper configuration (default=None)
        """
        check_type(processor_name, "processor_name", str)
        config = self._get_import_data_processor_config(processor_name, processor_code, raw_config)

        self.session.create_tenant_appendix(
            TenantAppendixCategory.IMPORT_DATA_PROCESSORS,
            config,
            entry_id=processor_name,
        )

    def get_import_data_processor(self, processor_name: str) -> str:
        """Gets an import data processor entry.

        Args:
            processor_name (str): Import data mapper name

        Returns:
            Specified import data_mapping configuration
        """
        check_type(processor_name, "processor_name", str)
        encoded = (
            self.session.get_tenant_appendix(
                TenantAppendixCategory.IMPORT_DATA_PROCESSORS,
                processor_name,
            )
            .get("content")
            .get("code")
        )
        return base64.b64decode(encoded).decode("utf-8")

    def update_import_data_processor(
        self, processor_name: str, processor_code: str = None, raw_config: dict = None
    ):
        """Updates an import data processor entry.

        Args:
            processor_name (str): Import data processor name
            processor_code (str): Unencoded import data processor code (default=None)
            raw_config (dict): New import data processor code (default=None)
        """
        check_type(processor_name, "processor_name", str)
        config = self._get_import_data_processor_config(processor_name, processor_code, raw_config)
        self.session.update_tenant_appendix(
            TenantAppendixCategory.IMPORT_DATA_PROCESSORS,
            config,
            processor_name,
        )

    @classmethod
    def _get_import_data_processor_config(
        cls, processor_name: str, processor_code: str, raw_config: dict
    ):
        """Validates import data processor config"""
        if processor_code or (raw_config or {}).get("encoding") == "plain":
            source = processor_code or raw_config.get("code")
            check_type(source, "processor_code", str)
            config = {
                "processorName": processor_name,
                "encoding": "Base64",
                "code": base64.b64encode(bytes(source, "utf-8")).decode("utf-8"),
            }
        elif raw_config:
            check_type(raw_config, "raw_config", dict)
            config = raw_config
        else:
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT,
                "Parameter 'processor_code' or 'raw_config' must be set",
            )
        return config

    def delete_import_data_processor(self, processor_name: str):
        """Deletes an import data processor entry.

        Args:
            processor_name (str): Import data mapper name
        """
        check_type(processor_name, "processor_name", str)
        self.session.delete_tenant_appendix(
            TenantAppendixCategory.IMPORT_DATA_PROCESSORS,
            processor_name,
        )

    def discover_import_subjects(self, import_source_id: str, timeout: int = None) -> dict:
        """Resolves the schema of an import source.

        Args:
            import_source_id (str): Import source configuration ID
            timeout (int): Timeout second

        Returns: dict: Discovered import source schema
        """
        check_type(import_source_id, "import_source_id", str)
        if timeout is not None:
            check_type(timeout, "timeout", int)
        return self.session.discover_import_subjects(import_source_id, timeout)

    def create_user(self, user: User):
        """Creates a user.

        Args:
            user (User): User information

        Returns: dict: Configuration of the created user
        """
        return self.session.create_user(user.to_dict())

    def get_users(self, emails: List[str] = None) -> dict:
        """Get users.
        Args:
            emails (list[str]): (Optional) Email addresses of users to list

        Returns: dict: Users info object
        """
        if emails is not None:
            check_type(emails, "emails", list)
            for email in emails:
                check_type(email, "email", str)
        return self.session.get_users(emails=emails)

    def modify_user(
        self,
        user_id: str,
        full_name: str = None,
        roles: List[str] = None,
        status: str = None,
    ):
        check_type(user_id, "user_id", str)
        return self.session.modify_user(user_id, full_name, roles, status)

    def delete_user(self, email: str = None, user_id: int = None):
        """Deletes a user.

        One of the parameters email or user_id must be set.
        If both are specified, the user_id would take effect and the email is ignored.

        Args:
            email (str): Email address of the deleting user
            user_id (int): ID of the deleting user
        """
        if email:
            check_type(email, "email", str)
        if user_id:
            check_type(user_id, "user_id", int)
        self.session.delete_user(email, user_id)

    def register_apps_service(
        self,
        tenant_name: str,
        hosts: List[str] = None,
        control_port: int = None,
        webhook_port: int = None,
    ) -> dict:
        """Registers biOS Apps service for a tenant.

        Args:
            tenant_name (str): Name of the tenant to register the service
            hosts (List[str]): Host names where the biOS Apps service runs (optional)
            control_port (int): biOS Apps control port number (optional).
                                If omitted, the server allocates an available one.
            webhook_port (int): biOS Apps webhook port number (optional).
                                If omitted, the server allocates an available one.
        Returns: dict: Registered biOS Apps information
        """
        check_not_empty_string(tenant_name, "tenant_name")
        if hosts is not None:
            check_str_list(hosts, "hosts", False)
        if control_port is not None:
            check_type(control_port, "control_port", int)
        if webhook_port is not None:
            check_type(webhook_port, "webhook_port", int)

        return self.session.register_apps_service(tenant_name, hosts, control_port, webhook_port)

    def get_apps_info(self, tenant_name: str) -> dict:
        """Retrieves biOS Apps service information.

        Args:
            tenant_name (str): Name of the tenant to query
        Returns: dict: The biOS Apps information
        """
        check_not_empty_string(tenant_name, "tenant_name")
        return self.session.get_apps_info(tenant_name)

    def deregister_apps_service(self, tenant_name: str):
        """De-registers biOS Apps service for a tenant.

        Args:
            tenant_name (str): Name of the tenant to query
        """
        check_not_empty_string(tenant_name, "tenant_name")
        return self.session.deregister_apps_service(tenant_name)

    def change_password(
        self,
        current_password: str = None,
        new_password: str = None,
        email: str = None,
    ):
        """Changes password.

        Args: This method takes keyword-only arguments
            new_password (str): New password -- always required
            current_password (str): Current password -- required for self password change
            email (str): Email address of the target user
                                                     -- required for cross-user password change
        """
        check_type(new_password, "new_password", str)
        if current_password is not None:
            check_type(current_password, "current_password", str)
        if email is not None:
            check_type(email, "email", str)
        self.session.change_password(current_password, new_password, email)

    def get_property(self, key):
        """Method to get a system property.

        Super admin privilege is required to execute this method.

        Args:
            key (str): property key.

        Returns:
            str: Property value.
                If the specified property is not set, an empty string is returned.
        """
        check_not_empty_string(key, "key")
        raw_value = self.session.get_property(key)
        return base64.b64decode(raw_value).decode("utf-8")

    def set_property(self, key, value):
        """Method to set a system property.

        Super admin privilege is required to execute this method.

        Args:
            key (str): property key.
            value (str): property value.
        """
        check_not_empty_string(key, "key")
        check_type(value, "value", str)
        encoded = f'"{base64.b64encode(bytes(value, "utf-8")).decode("utf-8")}"'
        return self.session.set_property(key, encoded)

    def maintain_keyspaces(
        self,
        action: str,
        include_dropped: bool = None,
        limit: int = None,
        keyspaces: List[str] = None,
        tenants: List[str] = None,
    ) -> dict:
        """Method to maintain keyspaces for tenants

        Args:
            action (str): Maintenance action. SUMMARIZE, LIST, SHOW, or DROP
            include_dropped (bool): Flag to include dropped keyspaces (optional, default=False)
            limit (int): Limits number of tables to maintain (optional)
            keyspaces (List[str]): Specifies keyspaces to maintain (optional)
            tenants (List[str]): Specifies tenants to maintain (optional)

        Returns:
            keyspaces info
        """
        check_not_empty_string(action, "action")
        if include_dropped is not None:
            check_type(include_dropped, "include_dropped", bool)
        if limit is not None:
            check_type(limit, "limit", int)
        if keyspaces is not None:
            check_type(keyspaces, "keyspaces", list)
            for i, name in enumerate(keyspaces):
                check_type(name, f"keyspaces[{i}]", str)
        if tenants is not None:
            check_type(tenants, "tenants", list)
            for i, name in enumerate(tenants):
                check_type(name, f"tenants[{i}]", str)

        request = {
            "action": action.upper(),
            "includeDroppedKeyspaces": include_dropped,
            "limit": limit,
            "keyspaces": keyspaces,
            "tenants": tenants,
        }
        return self.session.maintain_keyspaces(request)

    def maintain_tables(
        self, action: str, keyspace: str, limit: int = None, tables: List[str] = None
    ) -> dict:
        """Method to return keyspaces for tenants

        Args:
            action (str): Maintenance action. SUMMARIZE, LIST, SHOW, or DROP
            keyspace (str): Name of the keyspace to maintain
            limit (int): Limits number of tables to maintain
            tables (List[str]): Specifies tables to maintain

        Returns:
            keyspaces info
        """
        check_not_empty_string(action, "action")
        check_not_empty_string(keyspace, "keyspace")
        if limit is not None:
            check_type(limit, "limit", int)
        if tables is not None:
            check_type(tables, "names", list)
            for i, name in enumerate(tables):
                check_type(name, f"name[{i}]", str)

        request = {
            "keyspace": keyspace,
            "action": action.upper(),
            "limit": limit,
            "tables": tables,
        }
        return self.session.maintain_tables(request)

    def maintain_context(
        self,
        action: str,
        tenant: str,
        context: str,
        gc_grace_seconds: int = None,
        batch_size: int = None,
    ) -> dict:
        """Method to return keyspaces for tenants

        Args:
            action (str): Maintenance action. DESCRIBE, CHANGE_GC_GRACE_SECONDS, or CLEANUP
            tenant (str): Tenant name
            context (str): Context name

        Returns:
            dict: Context maintenance report
        """
        check_not_empty_string(action, "action")
        check_not_empty_string(tenant, "tenant")
        check_not_empty_string(context, "context")
        if gc_grace_seconds is not None:
            check_type(gc_grace_seconds, "gc_grace_seconds", int)
        if batch_size is not None:
            check_type(batch_size, "batch_size", int)

        request = {
            "action": action.upper(),
            "tenantName": tenant,
            "contextName": context,
            "gcGraceSeconds": gc_grace_seconds,
            "batchSize": batch_size,
        }
        return self.session.maintain_context(request)

    def get_report_configs(self, report_ids: List[str] = None, detail=None) -> dict:
        """Method to get the list of reports in this tenant.

        Args:
            report_ids (List[str]): list of reports to return;
                                    if absent, returns all reports in the tenant.
            detail (bool): retrieves report configuration details if true
        """
        if report_ids is not None:
            check_str_list(report_ids, "report_ids")
        if detail is not None:
            check_type(detail, "detail", bool)
        return self.session.get_report_configs(report_ids, detail)

    def put_report_config(self, report: dict) -> None:
        """Method to create or update a report with configuration as passed in report param.

        Args:
            report (dict): the report configuration as a dictionary.
        """
        check_type(report, "report", [dict])
        self.session.put_report_config(report)

    def delete_report(self, report_id: str) -> None:
        """Method to delete a report.

        Args:
            report_id (str): the report to be deleted.
        """
        check_not_empty_string(report_id, "report_id")
        self.session.delete_report(report_id)

    def get_insight_configs(self, insight_name: str) -> Any:
        """Gets the last inserted insight config for this user.

        Args:
            insight_name (str): insight name

        Returns: Any: Retrieved insight config
        """
        check_not_empty_string(insight_name, "insight_name")
        return self.session.get_insight_configs(insight_name)

    def put_insight_configs(self, insight_name, insight_configs: Any):
        """Puts insight configs for the user.

        Args:
            insight_name (str): insight name
            insight_configs (Any): the insight configuration as a dictionary.
        """
        check_not_empty_string(insight_name, "insight_name")
        check_type(insight_configs, "insight_configs", Any)
        self.session.put_insight_configs(insight_name, insight_configs)

    def delete_insights(self, insight_name: str):
        """Deletes insights for the user.

        Args:
            insight_name (str): insight name
        """
        check_not_empty_string(insight_name, "insight_name")
        self.session.delete_insight_configs(insight_name)

    def feature_status(self, stream: str, feature: str = None) -> dict:
        """Method to get the status of a feature - time until when it has been updated.

        Args:
            stream (str): the stream the feature belongs to.
            feature (str): the feature to be checked.
        Returns: (dict)
            feature status
        """
        check_not_empty_string(stream, "stream")
        if feature is not None:
            check_not_empty_string(feature, "feature")
        return self.session.feature_status({"stream": stream, "feature": feature})

    def feature_refresh(self, stream: str, feature: str = None) -> None:
        """Method to request refreshing a feature - to bring it up-to-date.

        Args:
            stream (str): the stream the feature belongs to.
            feature (str): the feature to be refreshed.
        """
        check_not_empty_string(stream, "stream")
        if feature is not None:
            check_not_empty_string(feature, "feature")
        self.session.feature_refresh({"stream": stream, "feature": feature})

    def add_endpoint(self, endpoint, nodetype=NodeType.SIGNAL):
        """Method to add an endpoint to list of available service endpoints.

        The method does nothing if the specified endpoint already exists in
        the list. Endpoints are compared in case insensitive manner.

        Args:
            endpoint (str): the endpoint to be added to the list of available service endpoints.
            nodetype (NodeType): type of node (default=NodeType.SIGNAL).
        """
        check_not_empty_string(endpoint, "endpoint")
        self.session.add_endpoint(endpoint, nodetype)

    def list_endpoints(self):
        """Method to list available service endpoints.

        Returns:
            list of str: Available service endpoints
        """
        return self.session.list_endpoints()

    def delete_endpoint(self, endpoint):
        """Method to delete an endpoint from list of available service endpoints.

        The method does nothing if the specified endpoint does not exist in
        the list. Endpoints are compared in case insensitive manner.

        Args:
            endpoint (str): the endpoint to be added to the list of available service endpoints.
        """
        check_not_empty_string(endpoint, "endpoint")
        self.session.delete_endpoint(endpoint)

    def register_for_service(self, domain: str, email: str, service_type: str):
        """Method to register for a service

        Args:
            domain (str): Domain name of the subscriber
            email (str): Email address of the user
            service_type (str): Service type, currently 'StoreEnhancement' is available
        """
        check_not_empty_string(domain, "domain")
        check_not_empty_string(email, "email")
        check_not_empty_string(service_type, "service_type")
        return self.session.register_for_service(domain, email, service_type)

    def stores_query(
        self,
        domain: str = None,
        tenant_name: str = None,
        session_id: str = None,
        client_id: str = None,
        product_id: str = None,
        queries: List[dict] = None,
    ):
        if domain is not None:
            check_not_empty_string(domain, "domain_name")
        if tenant_name is not None:
            check_not_empty_string(tenant_name, "tenant_name")
        if session_id is not None:
            check_not_empty_string(session_id, "session_id")
        if client_id is not None:
            check_not_empty_string(client_id, "client_id")
        if product_id is not None:
            check_not_empty_string(product_id, "product_id")
        check_type(queries, "queries", list)
        for i, query in enumerate(queries):
            check_not_empty_string(query.get("queryType"), f"queries[{i}].queryType")
            if query.get("maxItems") is not None:
                check_type(query.get("maxItems"), f"queries[{i}].maxItems", int)

        return self.session.stores_query(
            domain, tenant_name, session_id, client_id, product_id, queries
        )

    def send_log_message(self, message: str):
        """Sends a log message to server.

        This method is available only when the server runs in test mode.

        Args:
            message (str): Log message
        """
        check_not_empty_string(message, "message")
        return self.session.send_log_message(message)

    def for_tenant(self, tenant_name: str) -> Delegate:
        """Generates a delegate for another tenant"""
        return Delegate(self, tenant_name)


def login(
    endpoint: str,
    email: str,
    password: str,
    app_name: str = None,
    app_type: AppType = None,
    cafile: str = None,
    timeout: Union[int, float] = None,
) -> Client:
    """Sign in and start a session.

    Call this method first to start accessing the biOS server.

    Args:
        endpoint (str): Server endpoint URL
        email (str): User's email address
        password (str): User's password
        app_name (str): (Optional) Application name
        app_type (AppType): (Optional) Application type. The type would be 'UNKNOWN' if omitted.
            Possible values are AppType.REALTIME, AppType.BATCH, and AppType.ADHOC
        cafile (str): (Optional) Path to a file of concatenated CA certificates in PEM format.
            If this parameter is omitted, environment variable SSL_CERT_FILE is referred to get
            the file path to the certificate file.
        timeout (int/float): (Optional) Operation timeout seconds
    Returns:
       Client: biOS client object with logged in state.
    """
    check_not_empty_string(endpoint, "endpoint")
    check_not_empty_string(email, "email")
    check_type(password, "password", str)
    if app_name is not None:
        check_not_empty_string(app_name, "app_name")
    if app_type is not None:
        check_type(app_type, "app_type", [AppType, str])
    if cafile is not None:
        check_not_empty_string(cafile, "cafile")
    timeout_ms = None
    if timeout is not None:
        check_type(timeout, "timeout", [int, float])
        timeout_ms = int(timeout * 1000)
    parse_result = urlparse(endpoint)
    port = parse_result.port if parse_result.port else 443
    client = Client(
        parse_result.hostname,
        port,
        parse_result.scheme == "https",
        cafile=cafile,
        timeout=timeout_ms,
    )
    client.login(email, password, app_name, app_type)
    return client


def enable_threads():
    """Enables multi-thread support"""
    BiosCSdkSession.enable_threads()


def disable_threads():
    """Disables multi-thread support"""
    BiosCSdkSession.disable_threads()
