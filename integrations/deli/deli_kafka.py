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
"""deli_kafka: The deli framework for kafka"""

import logging
import signal

from deli import prepare_framework
from deli.configuration import SourceType

DELI = None


def signal_handler(sig, frame):
    if DELI:
        DELI.shutdown()
    logging.info("Bye")


try:
    DELI = prepare_framework(source_type=SourceType.KAFKA)
    signal.signal(signal.SIGUSR1, signal_handler)
    DELI.bootstrap()
except Exception as err:
    logging.critical("FATAL ERROR: %s", err, exc_info=True)
