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
import re
from collections import OrderedDict
from typing import List, Union

import dpath

from ._proto._bios import data_pb2
from ._utils import check_str_list, check_type, json_to_csv
from .errors import ErrorCode, ServiceError
from .models.isql_request_message import ISqlRequestMessage, ISqlRequestType


class ISqlInsertRequest:
    """Bios ISqlInsert request class
    The class covers all isql insert statements.

    use :meth:`into` to specify signal or can specify the signal using into=signal in the
    constructor.

    use :meth:`csv` to specifiy the contents to be inserted.

    use :meth:`build` to build the request. build validates the request. Execute cannot be called.
        without building the request

    Args:
        into(str): (Optional) Name of the signal to be inserted into
        values(str or list or tuple): (Optional) values to be inserted
    """

    def into(self, into: str):
        """This method sets signal to insert event into
        Args:
            into(str): name of the signal

        Returns: ISqlInsertInto

        Raises:
            ServiceError: (INVALID_ARGUMENT) if the into argument is not a string
        """
        check_type(into, "into", str)
        return ISqlInsertInto(into)


class ISqlInsertInto:
    """This class sets signal to insert event into
    Args:
        into(str): name of the signal

    Returns: ISqlInsertInto

    Raises:
        ServiceError: (INVALID_ARGUMENT) if the into argument is not a string
    """

    def __init__(self, into):
        check_type(into, "into", str)
        self.signal = into
        self.re_for_csv = re.compile('[,"\\s]')

    def csv(self, csv: str):
        """This method sets event to be inserted into the signal.

        Args:
            values: (str) a string with comma separeted values

        Returns:  ISqlInsertCsv

        Raises:
            ServiceError: (INVALID_ARGUMENT) if validation fails

        """
        check_type(csv, "csv", str)
        return ISqlInsertCsv(self.signal, csv)

    def csv_bulk(self, csv_list: List[str]):
        """This method sets multiple events to be inserted into the signal.

        Args:
            values: (list of string) values for bulk insert

        Returns:  ISqlInsertCsvBulk

        Raises:
            ServiceError: (INVALID_ARGUMENT) if validation fails

        """
        check_str_list(csv_list, "csv_list", False)
        return ISqlInsertCsvBulk(self.signal, csv_list)

    def _convert_json_value_to_csv_field(self, data, mapping):
        def data_func(val):
            if isinstance(val, bool):
                return str(val).lower()
            if isinstance(val, str) and "," in val:
                return '"' + val + '"'
            return val

        return ",".join(
            [(str(data_func(dpath.get(data, val, default="")))) for key, val in mapping.items()]
        )

    def json(
        self,
        data: Union[str, dict],
        mapping: Union[None, str, OrderedDict],
    ):
        """
        This method sets json value to be inserted into the signal

        We get json from webhook interfaces. This method can build insert requests
        from json values.

        examples:
        lets say you have a json which is something like below. There are two carts, each cart
        has three items. Each item has a user, a thing and a quantity. We want to insert all
        the items into a signal that looks like this:
        '{
            "signalName": "testSignal",
            "missingAttributePolicy": "StoreDefaultValue",
            "attributes": [
              {
                "attributeName": "userName",
                "type": "String",
                "default": "UNKNOWN"
              },
              {
                "attributeName": "itemName",
                "type": "String",
                "allowedValues":["Crocin","Dolo650", "Betadine","UNKNOWN"],
                "default": "UNKNOWN"
              },
              {
                "attributeName": "itemQuantity",
                "type": "Integer",
                "default": "0"
              }
            ]
        }'
        Basically, the signal has these three attributes.
        We can insert the below json into the signal with a mapping json. The mapping json could
        look like this, using call
        request = isql().insert().into("testSignal").json(data, mapping).build()
        session.execute(request)
        mapping = "
        {
            "commonKeys": "carts/*/items/*",
            "data": {
                "userName": "user",
                "itemName": "item",
                "itemQuantity": "quantity"
            }
        }
        "

        data = '
        {
            "carts": [
                {
                    "items": [
                        {
                            "user": "Manish",
                            "item": "Crocin",
                            "quantity": "6"
                        },
                        {
                            "user": "Manish",
                            "item": "Dolo650",
                            "quantity": "10"
                        },
                        {
                            "user": "Manish",
                            "item": "Betadine",
                            "quantity": "1"
                        }
                    ]
                },
                {
                    "items": [
                        {
                            "user": "Aditya",
                            "item": "Crocin",
                            "quantity": "5"
                        },
                        {
                            "user": "Aditya",
                            "item": "Dolo650",
                            "quantity": "12"
                        },
                        {
                            "user": "Aditya",
                            "item": "Betadine",
                            "quantity": "3"
                        }
                    ]
                }

            ]
        }
        '
        This method also allows execution of code snippets which are passed in the mapping.
        For example, if you want to create a string like user bought item, you can create a mapping
        that looks like below.
        double_nested_mapping = {
            "commonKeys": ["carts", "*", "items", "*"],
            "data":
                OrderedDict([
                    ("userName", "user"),
                    ("itemName", {
                        "keys": ["item2"],
                        "transform": "lambda x : x",
                        "default": "UNKNOWN"
                    }),
                    ("itemString", {
                        "keys": ["item", "user", "quantity"],
                        "transform":
                            '''lambda i, u, q: f"user {u} has bought {q} {i}" '''
                    }),
                    ("itemQuantity", "quantity")
                ]),
        }
        In the above mapping, the transform for itemName just provides a default if item2 is not
        present in the json. The transform itemString creates string of format 'user Manish has
        bought 6 crocin' and inserts it into itemString in the signal.

        We can also insert the index of the items if we choose to do so. Creating a mapping
        similar to the one below will add the index information to the event. Please note that
        the string inside index should match the key in the common keys,
        for example index(carts) and not index(cart).
        mapping = "
        {
            "commonKeys": ["carts", "*", "items", "*"],
            "data": {
                "cartIndex": "index(carts)",
                "itemIdx": "index(items)",
                "userName": "user",
                "itemName": "item",
                "itemQuantity": "quantity"
            }
        }
        "
        Args:
            data:  JSON data string or dictionary, which will have fields to be
                   mapped to a signal
            mapping: JSON mapping string or a dictionary which will have signal
                    fields as keys and json fields as values.

        Returns: returns IsqlInsertCsv or IsqlInsertCsvBulk depending on if the
                 mapping contains an array or not.
        :raises ServiceError: INVALID_ARGUMENT if parsing of json and mapping fails.
        """
        csv_str, lst = json_to_csv(data, mapping)
        if lst:
            return self.csv_bulk(lst)

        return self.csv(csv_str)

    def _escaped_for_csv(self, src: Union[str, int, float, bool, bytes]):
        if not isinstance(src, str):
            return str(src)
        if self.re_for_csv.search(src) is not None:
            return '"' + src.replace('"', '""') + '"'

        return src

    def _csv_from_values(self, values: List[Union[str, int, float, bool, bytes]]):
        if values is not None:
            check_type(values, "values", list)
            delimiter = ""
            csv = ""
            for i, val in enumerate(values):
                check_type(val, f"values[{i}]", (str, int, float, bool, bytes))
                csv = csv + delimiter + self._escaped_for_csv(val)
                delimiter = ","
            return csv
        return None

    def values(self, values: List[Union[str, int, float, bool, bytes]]):
        """This method sets event to be inserted into the signal.

        Args:
            values: list of values to be inserted.

        Returns:  ISqlInsertCsv

        Raises:
            ServiceError: (INVALID_ARGUMENT) if validation fails

        """
        check_type(values, "values", list)
        return self.csv(self._csv_from_values(values))

    def values_bulk(self, values_list: List[List[Union[str, int, float, bool, bytes]]]):
        """This method sets multiple events to be inserted into the signal.

        Args:
            values_list: list of list of values to be inserted.
            Each list corresponds to one event in the signal.

        Returns:  ISqlInsertCsvBulk

        Raises:
            ServiceError: (INVALID_ARGUMENT) if validation fails

        """
        check_type(values_list, "values_list", list)
        csv_list = []
        for i, val in enumerate(values_list):
            check_type(val, f"values_list[{i}]", list)
            csv_list.append(self._csv_from_values(val))
        return self.csv_bulk(csv_list)


class ISqlInsertCsv:
    """This class sets event to be inserted into the signal.

    Args:
        values: list of values to be inserted.

    Returns:  ISqlInsertRequest: Self

    Raises:
        ServiceError: (INVALID_ARGUMENT) if validation fails

    """

    def __init__(self, signal, values):
        check_type(values, "values", str)
        self.type = True
        self.values = values
        self.signal = signal

    def build(self):
        """This method builds and validates insert statement.
        Args:
            None
        returns: ISqlInsertCompleteRequest:  Self

        Raises:
            ServiceError if validation fails.
        """
        return ISqlInsertCompleteRequest(self)


class ISqlInsertCsvBulk:
    """This class sets events to be inserted into the signal.

    Args:
        values: list of values to be inserted.

    Returns:  ISqlInsertRequest: Self

    Raises:
        ServiceError: (INVALID_ARGUMENT) if validation fails

    """

    def __init__(self, signal, values):
        check_type(values, "values", (list, tuple))
        if len(values) > 100000:
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT,
                "Cannot insert more than 100000 bulk events",
            )
        self.type = True
        self.values = values
        self.signal = signal

    def build(self):
        """This method builds and validates insert statement.
        Args:
            None
        returns: ISqlInsertCompleteRequest:  Self

        Raises:
            ServiceError if validation fails.
        """
        return ISqlInsertBulkCompleteRequest(self)


class ISqlInsertCompleteRequest(ISqlRequestMessage):
    """This class builds and validates insert statement.
    Args:
        ISqlInsertCsv
    returns: ISqlInsertCompleteRequest

    Raises:
        ServiceError if validation fails.
    """

    def __init__(self, isql_insert_csv):
        super().__init__(ISqlRequestType.INSERT)
        self.isql_insert_csv = isql_insert_csv
        self.signal = self.isql_insert_csv.signal
        self.values = self.isql_insert_csv.values

        if self.isql_insert_csv.signal is None:
            raise ServiceError(
                ErrorCode.INVALID_STATE,
                "Validation Failed: Signal is not set",
            )
        if self.isql_insert_csv.values is None:
            raise ServiceError(
                ErrorCode.INVALID_STATE,
                "Validation Failed: Event values not set",
            )
        self.request = data_pb2.InsertRequest()
        self.request.content_rep = data_pb2.ContentRepresentation.CSV
        self.request.record.string_values.append(self.isql_insert_csv.values)
        if not self.request.IsInitialized():
            raise ServiceError(
                ErrorCode.INVALID_STATE,
                "Validation failed: Invalid record values",
            )

        self.req_str = self.request.SerializeToString()
        if not len(self.req_str) > 0:
            raise ServiceError(
                ErrorCode.INVALID_STATE,
                "Validation failed: Could not convert values to binary format",
            )


class ISqlInsertBulkCompleteRequest(ISqlRequestMessage):
    """This class builds and validates insert statement.
    Args:
        ISqlInsertCsv
    returns: ISqlInsertCompleteRequest

    Raises:
        ServiceError if validation fails.
    """

    def __init__(self, isql_insert_csv):
        super().__init__(ISqlRequestType.INSERT_BULK)
        self.isql_insert_csv = isql_insert_csv
        self.signal = self.isql_insert_csv.signal
        self.values = self.isql_insert_csv.values
        self.content_representation = data_pb2.ContentRepresentation.CSV

        if self.isql_insert_csv.signal is None:
            raise ServiceError(
                ErrorCode.INVALID_STATE,
                "Validation Failed: Signal is not set",
            )
        if self.isql_insert_csv.values is None:
            raise ServiceError(
                ErrorCode.INVALID_STATE,
                "Validation Failed: Event values not set",
            )
        # We don't encode to protobuf message(s) yet; The bulk insert client does
        # it later on execution
