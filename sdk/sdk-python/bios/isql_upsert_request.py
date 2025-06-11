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
import json

from bios.errors import ErrorCode, ServiceError

from ._utils import check_str_list, check_type, json_to_csv
from .models import ISqlRequestMessage, ISqlRequestType


class ISqlUpsertRequest:
    """Bios ISqlUpsert request class
    It is used to create or update context entry.
    example:
    req = ISqlRequest().upsert().into("contextName").csv("csv,values").build()
    session.execute(req)
    """

    def into(self, into):
        """This method sets context to upsert context into
        Args:
            into(str): name of the context

        Returns: ISqlUpsertInto

        Raises:
            ServiceError: (INVALID_ARGUMENT) if the into argument is not a string
        """
        check_type(into, "into", str)
        return ISqlUpsertInto(into)


class ISqlUpsertInto:
    """This class sets context to upsert context into
    Args:
        into(str): name of the context

    Returns: ISqlUpsertInto

    Raises:
        ServiceError: (INVALID_ARGUMENT) if the into argument is not a string
    """

    def __init__(self, into):
        check_type(into, "into", str)
        self.context = into

    def csv(self, csv):
        """This method sets context to be upserted into the context.

        Args:
            values: (str) a string with comma separeted values

        Returns:  ISqlUpsertCsv

        Raises:
            ServiceError: (INVALID_ARGUMENT) if validation fails

        """
        return self._csv_bulk(csv)

    def json(self, data, mapping):
        data_str, lst = json_to_csv(data, mapping)
        if lst:
            return self.csv_bulk(lst)

        return self.csv(data_str)

    def csv_bulk(self, csv):
        """This method sets context to be upserted into the context.

        Args:
            csv: (List(str)) a list of strings with comma separeted values

        Returns:  ISqlUpsertCsv

        Raises:
            ServiceError: (INVALID_ARGUMENT) if validation fails

        """
        return self._csv_bulk(*csv)

    def _csv_bulk(self, *csv):
        """This method sets context to be upserted into the context.

        Args:
            values: (str) a string with comma separeted values

        Returns:  ISqlUpsertCsv

        Raises:
            ServiceError: (INVALID_ARGUMENT) if validation fails

        """
        if isinstance(csv, (list, tuple)):
            csv = list(csv)
            check_str_list(csv, "csv", False)
            return ISqlUpsertCsv(self.context, csv)

        check_type(csv, "csv", str)
        return ISqlUpsertCsv(self.context, csv)


class ISqlUpsertCsv:
    """This class sets context to be upserted into the context.

    Args:
        values: list of values to be upserted.

    Returns:  ISqlUpsertRequest: Self

    Raises:
        ServiceError: (INVALID_ARGUMENT) if validation fails

    """

    def __init__(self, context, values):
        self.values = values
        self.context = context

    def build(self):
        """This method builds and validates upsert statement.
        Args:
            None
        returns: ISqlUpsertCompleteRequest:  Self

        Raises:
            ServiceError if validation fails.
        """
        return ISqlUpsertCompleteRequest(self)


class ISqlUpsertCompleteRequest(ISqlRequestMessage):
    """This class builds and validates upsert statement.
    Args:
        ISqlUpsertCsv
    returns: ISqlUpsertCompleteRequest

    Raises:
        ServiceError if validation fails.
    """

    def __init__(self, isql_upsert_csv):
        super().__init__(ISqlRequestType.UPSERT)
        self.isql_upsert_csv = isql_upsert_csv
        self.context = self.isql_upsert_csv.context
        self.values = self.isql_upsert_csv.values

        if self.isql_upsert_csv.context is None:
            raise ServiceError(
                ErrorCode.INVALID_STATE,
                "Validation Failed: Signal is not set",
            )
        if self.isql_upsert_csv.values is None:
            raise ServiceError(
                ErrorCode.INVALID_STATE,
                "Validation Failed: Event values not set",
            )
        if not len(self.values) > 0:
            raise ServiceError(
                ErrorCode.INVALID_STATE,
                "Validation failed: Could not convert values to binary format",
            )

    def __repr__(self):
        return json.dumps({"contentRepresentation": "CSV", "entries": self.values})
