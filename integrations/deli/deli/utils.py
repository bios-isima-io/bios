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
import datetime
import logging
import os
import time
from configparser import ConfigParser
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Union

import bios
from bios import ErrorCode, ServiceError

# global logger for static functions
mod_logger = logging.getLogger(__name__)


def get_time():
    """return the utc timestamp in seconds"""
    now = datetime.now(timezone.utc)
    utc_time = now.replace(tzinfo=timezone.utc)
    utc_timestamp = utc_time.timestamp()
    return int(utc_timestamp)


def chunks(lst, size):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def time_to_sleep(seconds: int):
    """calculate the to sleep in seconds till the next slot which is multiple
    of minutes boundaries, ie 5, 10, 15 ..."""
    minutes = seconds // 60
    now = datetime.now()
    next_time = now + (datetime.min - now) % timedelta(minutes=minutes)
    duration = (next_time - now).total_seconds()
    return duration


def bios_get_session(endpoint, user, password, ca, app_name, app_type):
    """bios login with  3 retries
    retries cannot be done with context managers
    don't try that.
    """
    login_retries = 3
    login_delay = 5
    while True:
        try:
            return bios.login(
                endpoint,
                user,
                password,
                cafile=ca,
                app_name=app_name,
                app_type=app_type,
            )
        except ServiceError as err:
            if login_retries > 0 and err.error_code not in (
                ErrorCode.UNAUTHORIZED,
                ErrorCode.FORBIDDEN,
            ):
                mod_logger.warning(
                    "Failed to create a biOS session, retrying in %s seconds; user=%s, error=%s, message=%s",
                    login_delay,
                    user,
                    err.error_code,
                    err.message,
                )
                login_retries -= 1
                time.sleep(login_delay)
                login_delay *= 2
            else:
                mod_logger.error(
                    "Failed to create a biOS session, retrying in %s seconds; user=%s, error=%s, message=%s",
                    login_delay,
                    user,
                    err.error_code,
                    err.message,
                )
                time.sleep(login_delay)


def bios_get_auth_params(system_config: ConfigParser, user_type: str = None) -> Tuple[str, str]:
    """get the auth params i.e user and password"""
    CONFIG_SECTION = "PullConfigLoader"
    tenant = system_config.get(CONFIG_SECTION, "tenant", fallback=None)

    if user_type:
        user_property_name = f"user_{user_type}"
        password_property_name = f"password_{user_type}"
        user = system_config.get(CONFIG_SECTION, user_property_name, fallback=None)
        password = system_config.get(CONFIG_SECTION, password_property_name, fallback=None)
        if user and password:
            return user, password

    user = system_config.get(CONFIG_SECTION, "user", fallback=None)
    password = system_config.get(CONFIG_SECTION, "password", fallback=None)
    # If only tenant is specified, we use the tenant's support account
    if not user and tenant:
        user = f"support+{tenant}@isima.io"
        password = os.getenv("BIOS_PASSWORD", "Test123!")
        if len(password) == 0:
            password = "Test123!"
    return user, password


def bios_login(system_config: ConfigParser):
    """bios login  given system config"""
    CONFIG_SECTION = "PullConfigLoader"
    bios_endpoint = system_config.get(CONFIG_SECTION, "endpoint")
    bios_cafile = system_config.get(CONFIG_SECTION, "sslCertFile", fallback=None)
    app_name = system_config.get("Common", "appName", fallback=None)
    app_type = system_config.get("Common", "appType", fallback=None)
    bios_user, bios_password = bios_get_auth_params(system_config)
    session = bios_get_session(
        bios_endpoint, bios_user, bios_password, bios_cafile, app_name, app_type
    )
    if session is None:
        raise Exception(f"Unable to login, endpoint: {bios_endpoint} user : {bios_user}")
    return session


def coerce_types(key_values: dict, key_types: dict):
    """given a k,v pairs, coerce the values to given type specified in key_types"""
    conversions = {
        int: lambda x: int(x),
        float: lambda x: float(x),
        bool: lambda x: True if str(x).lower in ["true"] else False,
        str: lambda x: str(x),
    }
    result = key_values
    for k, v in key_values.items():
        if k in key_types:
            result[k] = conversions[key_types[k]](v)
    return result


def parse_attribute_path(attribute_path: str) -> Union[List[str], str]:
    """Parses attribute search path"""
    assert isinstance(attribute_path, str)
    elements = attribute_path.split("/")
    if "*" not in elements[:-1] and "[" not in elements[-1]:
        return elements
    else:
        return attribute_path
