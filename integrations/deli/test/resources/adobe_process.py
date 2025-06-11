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
import ast


def get_sft(props_str):
    prop_list = ast.literal_eval(str(props_str))
    for prop in prop_list:
        if prop["name"] == "sourceFirstTimestamp":
            return int(prop["value"]) * 1000
    return 0


def get_slt(props_str):
    prop_list = ast.literal_eval(str(props_str))
    for prop in prop_list:
        if prop["name"] == "sourceLastTimestamp":
            return int(prop["value"]) * 1000
    return 0


def get_tft(props_str):
    prop_list = ast.literal_eval(str(props_str))
    for prop in prop_list:
        if prop["name"] == "triggerFiredTimestamp":
            return int(prop["value"])
    return 0
