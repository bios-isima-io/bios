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

from ._utils import check_type
from .errors import ErrorCode, ServiceError
from .isql_delete_request import ISqlDeleteContextRequest
from .isql_insert_request import ISqlInsertRequest
from .isql_select_request import From, SelectContext
from .isql_update_context_request import ISqlUpdateContextRequest
from .isql_upsert_request import ISqlUpsertRequest
from .models.isql_request_message import ISqlRequestMessage, ISqlRequestType


class ISqlRequest:
    """Bios ISqlRequest request class.
    The class covers all isql statements.
    It can be used for insert, delete, select, updatestatements.
    example:
        req = ISqlRequest().select().from_signal("foo").where(clauses).build()
        responses = session.execute(req)
    """

    def __init__(self):
        pass

    def insert(self):
        """Sets up isql insert request message.
        This message is for inserting event(s)
        """
        return ISqlInsertRequest()

    def update(self, name: str):
        """Sets up isql update context messge.
        This message is for updating existing context
        """
        return ISqlUpdateContextRequest(name)

    def upsert(self):
        """Sets up upsert context message.
        This message is for insert/update for context(s)
        """
        return ISqlUpsertRequest()

    def delete(self):
        """Sets up delete context message.
        This message is for deleting context entries
        """
        return ISqlDeleteContextRequest()

    def select(self, *targets):
        """Sets up select message.
        This message is for selecting event(s)/context(s)
        """
        context = SelectContext(*targets)
        return From(context)


def isql() -> ISqlRequest:
    """Starts an iSQL statement"""
    return ISqlRequest()


class RollbackOnFailureISqlRequestMessage(ISqlRequestMessage):
    """Bios RollbackOnFailure request class"""

    def __init__(self, statement: ISqlRequestMessage):
        super().__init__(ISqlRequestType.ATOMIC_MUTATION)
        check_type(statement, "statement", ISqlRequestMessage)
        if statement.get_type() == ISqlRequestType.ATOMIC_MUTATION:
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT, "rollback_on_failure must not be nested"
            )
        self.statements = [statement]


def rollback_on_failure(statement: ISqlRequestMessage) -> RollbackOnFailureISqlRequestMessage:
    return RollbackOnFailureISqlRequestMessage(statement)
