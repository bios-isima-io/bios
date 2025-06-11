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

"""
Utilities module
"""
import csv
import datetime
import itertools
import json
import os
import pickle
from collections import OrderedDict
from enum import Enum
from io import StringIO
from typing import Any, Set, Union

import dpath

from .errors import ErrorCode, ServiceError


def check_type(param, param_name, expected_type):
    """Method that checks if a parameter is of expected type.

    Args:
        param (?): Parameter to test
        param_name (str): Parameter name to display on check failure
        expected_type (type): Expected type

    Raises:
        ServiceError: Raised with ErrorCode.INVALID_ARGUMENT when the
        parameter is not of expected type.
    """
    if expected_type == Any:
        if param is None:
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT,
                (f"Parameter '{param_name}' must not be None"),
            )
        return True

    if isinstance(expected_type, list):
        meet = False
        for typ in expected_type:
            if isinstance(param, typ):
                meet = True
                break
        if not meet:
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT,
                (
                    f"Parameter '{param_name}' must be of type {expected_type}, "
                    f"but {type(param)} was specified",
                ),
            )
    else:
        if not isinstance(param, expected_type):
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT,
                (
                    f"Parameter '{param_name}' must be of type {expected_type}, "
                    f"but {type(param)} was specified",
                ),
            )
    return True


def check_not_empty(param, param_name):
    """Method that checks if a parameter is not empty

    Args:
        param (?): Parameter to test
        param_name (str): Parameter name to display on check failure
        expected_type (type): Expected type

    Raises:
        ServiceError: Raised with ErrorCode.INVALID_ARGUMENT when the
        parameter is not of expected type.
    """
    if not param or (isinstance(param, str) and not param.strip()):
        raise ServiceError(
            ErrorCode.INVALID_ARGUMENT,
            f"Parameter '{param_name}' may not be empty",
        )
    return True


def check_not_empty_string(param, param_name):
    """Method that checks if a parameter is not empty string

    Args:
        param (?): Parameter to test
        param_name (str): Parameter name to display on check failure
        expected_type (type): Expected type

    Raises:
        ServiceError: Raised with ErrorCode.INVALID_ARGUMENT when the
        parameter is not of expected type.
    """
    check_type(param, param_name, str)
    if not param.strip():
        raise ServiceError(
            ErrorCode.INVALID_ARGUMENT,
            f"Parameter '{param_name}' may not be empty",
        )
    return True


def check_str_list(
    params,
    param_name,
    allow_empty=False,
    do_not_allow_duplicates=False,
):
    """Method that checks if elements in the list are none-empty strings

    Args:
        params (list): List parameter to check
        param_name (str): Parameter name to display on check failure
        allow_empty (bool): True if empty list is allowed

    Raises:
        ServiceError: Raised with ErrorCode.INVALID_ARGUMENT when the
                      validation fails.
    """
    check_type(params, param_name, list)
    if not allow_empty:
        check_not_empty(params, param_name)
    keys = set()
    for i, elem in enumerate(params):
        check_not_empty_string(elem, f"{param_name}[{i}]")
        if do_not_allow_duplicates:
            norm = elem.lower()
            if norm in keys:
                raise ServiceError(
                    ErrorCode.INVALID_ARGUMENT,
                    f"Duplicate values '{elem}' are in parameter '{param_name}'",
                )
            keys.add(norm)
    return True


def check_list(params, param_name, expected_type, allow_empty=False, allow_tuple=False):
    """Method that checks if elements in the list are of specified type

    Args:
        params (list): List parameter to check
        param_name (str): Parameter name to display on check failure
        expected_type (type): expected type
        allow_empty (bool): True if empty list is allowed

    Raises:
        ServiceError: Raised with ErrorCode.INVALID_ARGUMENT when the
                      validation fails.
    """
    if allow_tuple:
        check_type(params, param_name, (list, tuple))
    else:
        check_type(params, param_name, list)
    if not allow_empty and len(params) == 0:
        raise ServiceError(
            ErrorCode.INVALID_ARGUMENT,
            f"List parameter '{param_name}' may not be empty",
        )
    for i, elem in enumerate(params):
        check_type(elem, f"{param_name}[{i}]", expected_type)
    return True


def check_str_enum(value: str, value_name: str, allowed_values: Set[str], ignore_case: bool):
    """Method that checks whether a value is in a set of allowed values

    Args:
        value (str): Value to test
        value_name (str): Name of the value
        allowed_values (Set[str]): Allowed values
        ignore_case (bool): Flag to determine if the method ignores cases

    Raises:
        ServiceError: Raised with ErrorCode.INVALID_ARGUMENT when the
                      validation fails.
    """
    check_type(value, value_name, str)
    to_test = value.upper() if ignore_case else value
    allowed = {entry.upper() for entry in allowed_values} if ignore_case else allowed_values
    if to_test not in allowed:
        raise ServiceError(
            ErrorCode.INVALID_ARGUMENT,
            f"{value_name}: Value {value} is invalid. Allowed values are {allowed_values}",
        )


def _split_list_by_separator(common_map: list, separator="*"):
    if any(i == j for i, j in zip(common_map, common_map[1:])):
        raise ServiceError(
            ErrorCode.INVALID_ARGUMENT,
            "'*, *' is not allowed in mapping. We cannot have unnamed arrays",
        )
    count = sum(x == separator for x in common_map)
    result = [
        list(val)
        for inp, val in itertools.groupby(common_map, lambda z: z == separator)
        if not inp
    ]
    if count > len(result):
        raise ServiceError(
            ErrorCode.INVALID_ARGUMENT,
            "The number of keys are less than number "
            "of '*'s. Unnamed arrays are not "
            "supported in json insertion.",
        )
    for i in range(count):
        result[i].append(separator)
    return result


def _get_indexed_values_from_split_keys(data, common_keys, leaf_keys, indexes=None):
    if indexes is None:
        indexes = {}
    if len(common_keys) > 0:
        current_array = common_keys[0]
        lca = len(current_array)
        rest = common_keys[1:]
        item = current_array[lca - 2]  # last but one element
        for idx, value in enumerate(dpath.values(data, current_array)):
            indexes[f"index({item})"] = idx
            yield from _get_indexed_values_from_split_keys(value, rest, leaf_keys, indexes)
    else:
        yield {**indexes, **data}


def _get_indexed_values(data, mapping):
    common_keys = _split_list_by_separator(mapping["commonKeys"])
    return _get_indexed_values_from_split_keys(data, common_keys, mapping["data"])


def _get_transform(transform):
    if callable(transform):
        return transform
    try:
        return pickle.loads(transform)
    except TypeError:
        return eval(transform)  # pylint: disable=eval-used


def _lambda_transforms(data, lambda_map, default):
    if os.environ.get("BIOS_LAMBDA_DISABLED") == "true":
        raise ServiceError(
            ErrorCode.INVALID_REQUEST,
            "Dynamic code execution is not allowed in "
            "this environment. You can set environment "
            "variable BIOS_LAMBDA_DISABLED='False' to "
            "allow lambda execution",
        )
    values = [
        dpath.get(data, k, default=lambda_map.get("default", default)) for k in lambda_map["keys"]
    ]
    transform = _get_transform(lambda_map["transform"])
    return transform(*values)


def _transform(data, key, default=None):
    if isinstance(key, dict):
        return _lambda_transforms(data, key, default)
    if isinstance(key, (list, str)):
        return dpath.get(data, key, default=default)
    return default


def _convert_json_value_to_csv_field(data, mapping):
    def val_trans(val):
        if isinstance(val, bool):
            return str(val).lower()
        return val

    stream = StringIO()
    writer = csv.writer(
        stream,
        quoting=csv.QUOTE_ALL,
    )
    writer.writerow(
        [
            (
                str(val_trans(data[val]))
                if isinstance(val, str) and val in data
                else str(val_trans(_transform(data, val, default="")))
            )
            for key, val in mapping.items()
        ]
    )
    return stream.getvalue().strip()


def json_to_csv(
    data: Union[str, dict], mapping: Union[None, str, OrderedDict]
) -> tuple[str, list]:
    """Method that converts json string into csv/list of csv in order to insert/upsert it into
    bios.

        Args:
            data (str/dict): the json value that needs to be inserted.
            mapping (str/dict): the mapping dictionary that will be used to transform data.
                The mapping needs to have at least one field 'data' that will point to the
                data fields in the json. commonKeys are optional.
                example:
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

        Returns:
            string of csv value and
            list of comma separated strings.
            if the mapping contains an array, then it returns string as None and values
            in the list. If the mapping does not contain an array, then it returns list as
            None and string with values.

        Raises:
            ServiceError: Raised with ErrorCode.INVALID_ARGUMENT when the
                          validation fails.

    """
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError as exc:
            raise ServiceError(
                ErrorCode.BAD_INPUT,
                "The parameter 'data' is not a valid json",
            ) from exc

    if isinstance(mapping, str):
        try:
            mapping = json.loads(mapping)
        except json.JSONDecodeError as exc:
            raise ServiceError(
                ErrorCode.BAD_INPUT,
                "The parameter 'mapping' is not a valid json",
            ) from exc

    if not isinstance(data, dict):
        raise ServiceError(
            ErrorCode.BAD_INPUT,
            "Only types of str and dict are supported for json",
        )

    if not isinstance(mapping, dict):
        raise ServiceError(
            ErrorCode.BAD_INPUT,
            "Mapping must be of type json string or dictionary",
        )

    common_keys = mapping.get("commonKeys", None)
    try:
        description = mapping["data"]
    except KeyError as exc:
        raise ServiceError(
            ErrorCode.BAD_INPUT,
            "Mapping must have field 'data' with mapping keys in it.",
        ) from exc
    if not common_keys or len(common_keys) == 0:
        return (
            _convert_json_value_to_csv_field(data, description),
            None,
        )
    attr_values_from_root_path = {}
    for key, val in description.items():
        if "*" in val:
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT,
                "Leaf level map cannot contain wildcard '*'",
            )
        if isinstance(val, str) and val.startswith("/"):
            attr_values_from_root_path[key] = dpath.get(data, val, default="")
    if common_keys and "*" in common_keys:
        csv_list = []
        for row in _get_indexed_values(data, mapping):
            for key, val in attr_values_from_root_path.items():
                row[description[key]] = val
            csv_list.append(_convert_json_value_to_csv_field(row, description))
        return None, csv_list
    if common_keys:
        return (
            _convert_json_value_to_csv_field(dpath.get(data, common_keys), description),
            None,
        )
    return None, None


class Expandable:
    """Base class for JSON serializable object.

    This class provides expand() method that expands the self to a map
    recursively. The method is useful for JSON serialization.
    """

    def __init__(self):
        pass

    def __repr__(self):
        return json.dumps(self.expand())

    def expand(self):
        """Recursively expands the object to a dict"""
        obj = self.__dict__.copy()
        for key, value in obj.items():
            if isinstance(value, Expandable):
                obj[key] = value.expand()
            elif isinstance(value, list):
                for i, val in enumerate(value):
                    if isinstance(val, Expandable):
                        obj[key][i] = val.expand()
            elif isinstance(value, Enum):
                if hasattr(value, "data"):
                    obj[key] = value.data()
                else:
                    obj[key] = str(value)
            elif isinstance(value, datetime.timezone):
                obj[key] = str(value)
        return obj
