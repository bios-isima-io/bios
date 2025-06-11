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


def get_minimum_signal(signal_name=None):
    return load_signal_json("../resources/bios-signal-minimum.json", signal_name)


def get_all_types_signal(signal_name=None):
    return load_signal_json("../resources/bios-signal-all-types.json", signal_name)


def load_signal_json(json_file_name, signal_name=None):
    with open(json_file_name, "r") as json_file:
        signal = json.loads(json_file.read())
        if signal_name:
            signal["signalName"] = signal_name
        return signal


def next_checkpoint(current_time: int, interval: int) -> int:
    return int((current_time + interval - 1) / interval) * interval
