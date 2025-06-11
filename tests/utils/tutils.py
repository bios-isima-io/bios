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
Collection of test utility methods
"""

import base64
import datetime
import os
import pprint
import random
import time
from datetime import timezone

from bios import ServiceError
from tsetup import (
    admin_pass,
    admin_user,
    extract_pass,
    extract_user,
    get_rollup_intervals,
)
from tsetup import get_single_endpoint as endpoint
from tsetup import ingest_pass, ingest_user, sadmin_pass, sadmin_user


def run_negative_test(test, function, expected_error_code, expected_error_message):
    """Generic method that runs a negative test case"""
    try:
        function()
        test.fail("Exception is expected")
    except ServiceError as err:
        caught = err
    test.assertEqual(caught.error_code, expected_error_code)
    test.assertIn(expected_error_message, caught.message)


def b64enc(src):
    """Utility method that encodes a bytearray to base64 string.

    Args:
        src (bytearray): Input byte array

    Returns: str: base64 encoded string
    """
    return base64.b64encode(src).decode("utf-8")


def b64dec(src):
    """Utility method that decodes a base64 string to byte array

    Args:
        src (str): Base64 string

    Returns: bytearray: base64 decoded bytearray
    """
    return base64.b64decode(src)


def b64encStr(strg):
    """Utility method that encodes a bytearray to base64 string.

    Args:
        strg : Input String

    Returns: str: base64 encoded string
    """
    return base64.b64encode(strg.encode("utf-8")).decode("utf-8")


def summarize_checkpoint_floor(timestamp, time_span, timezone=timezone.utc):
    interval = time_span.value
    offset = int(timezone.utcoffset(None).total_seconds() * 1000)
    return int((timestamp + offset) / interval) * interval - offset


def get_next_checkpoint(timestamp, interval, timezone=timezone.utc):
    """Calculates the next checkpoint with the specified time span

    Args:
        timestamp (int): Current time stamp as seconds since Epoch
        interval (int, or datetime.timedelta): Checkpoint interval in seconds
        timezone (datetime.timezone): timezone

    Returns: int: Checkpoint as seconds since epoch
    """
    if isinstance(interval, datetime.timedelta):
        interval = int(interval.total_seconds())
    else:
        interval = int(interval)
    offset = int(timezone.utcoffset(None).total_seconds())
    return int((timestamp + offset + interval - 1) / interval) * interval - offset


def skip_interval(origin: int, interval: int = None):
    """Method to sleep until the next rollup interval.

    The method sleeps until the next rollup window from the origin time

    Args:
        origin (int): Origin milliseconds.
        interval (int): Interval milliseconds to skip.  The default interval is used
            if not speicfied.
    """
    if interval is None:
        interval = get_rollup_intervals()
    wakeup_time = origin + interval
    if wakeup_time % interval < 15000:
        wakeup_time += 15000
    sleep_time = wakeup_time / 1000 - time.time()
    if sleep_time > 0:
        print(f"Sleeping for {sleep_time} seconds")
        time.sleep(sleep_time)
