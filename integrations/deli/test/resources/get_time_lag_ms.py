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
import time


def get_time_lag_ms(in_event_s_or_ms):
    in_event_s_or_ms = in_event_s_or_ms or 0
    try:
        # Allow a small amount of margin for clock skews - an event can be upto this much in the future.
        margin_s = 60 * 60

        # First normalize to milliseconds since epoch.
        current_s = time.time()
        in_event_s_or_ms = int(in_event_s_or_ms)
        event_ms = 0
        if in_event_s_or_ms < current_s + margin_s:
            event_ms = in_event_s_or_ms * 1000
        else:
            if in_event_s_or_ms < (current_s + margin_s) * 1000:
                event_ms = in_event_s_or_ms
            else:
                # If input is more than than the margin in the future, it is bad data - return 0.
                return 0
        lag_ms = current_s * 1000 - event_ms

        # If lag is more than 100 days, the data may be bad - return 0.
        if lag_ms > 1000 * 60 * 60 * 24 * 100:
            return 0

        return int(lag_ms)
    except:
        return 0
