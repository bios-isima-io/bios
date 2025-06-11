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


def isNull(val):
    if val is None or val == "None" or val.lower() == "null":
        return True
    return False


def value_transform(val):
    if isNull(val):
        return ""

    try:
        val = str(int(datetime.datetime.strptime(val, "%Y-%m-%d %H:%M:%S").timestamp() * 1000))
    except:
        try:
            val = str(
                int(datetime.datetime.strptime(val, "%Y-%m-%d %H:%M:%S.%f").timestamp() * 1000)
            )
        except:
            val = ""

    return val


def value_transform_date_time_combination(date_val, time_val):
    if isNull(date_val) or isNull(time_val):
        return ""
    val = str(date_val[0:10]) + " " + str(time_val)
    return value_transform(val)


def composite_string_and_two_integers(s, i1, i2):
    s = s or ""
    i1 = i1 or 0
    i2 = i2 or 0
    return s + "_" + str(i1) + "_" + str(i2)


def timestamp_window_fifteen_minutes_epoch(input_epoch):
    input_epoch = int(input_epoch or 0)
    if len(str(input_epoch)) == 10:
        output_epoch = input_epoch * 1000
    else:
        output_epoch = input_epoch
    return (output_epoch % 86400000) - (output_epoch % 900000)


def to_millisecond_timestamp_epoch(input_epoch):
    input_epoch = int(input_epoch or 0)
    if len(str(input_epoch)) == 10:
        output_epoch = input_epoch * 1000
    else:
        output_epoch = input_epoch
    return output_epoch


def add_date_and_slno(s_date, sl_no):
    try:
        date_in_str = value_transform(s_date)
        date_in_str = date_in_str or "0"
        date_in_int = int(date_in_str)
        sl_no_int = sl_no or 0
        out = date_in_int + int(sl_no_int)
        return out
    except:
        return 0


def get_today(input_epoch):
    return int(86400 * ((int(input_epoch) / 1000 + 330 * 60) // 86400))


def atc_vertical(order_type, product_id):
    if order_type == "pathlab":
        return "-1"
    else:
        return str(product_id or 0)
