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

from enum import Enum, unique


@unique
class MetricFunction(Enum):
    """Enum class to be used for identifying a metric function."""

    SUM = 0
    COUNT = 1
    MIN = 2
    MAX = 3
    LAST = 4
    AVG = 5
    VARIANCE = 6
    STDDEV = 7
    SKEWNESS = 8
    KURTOSIS = 9
    SUM2 = 10
    SUM3 = 11
    SUM4 = 12
    MEDIAN = 31
    P0_01 = 32
    P0_1 = 33
    P1 = 34
    P10 = 35
    P25 = 36
    P50 = 37
    P75 = 38
    P90 = 39
    P99 = 40
    P99_9 = 41
    P99_99 = 42
    DISTINCTCOUNT = 61
    DCLB1 = 62
    DCUB1 = 63
    DCLB2 = 64
    DCUB2 = 65
    DCLB3 = 66
    DCUB3 = 67
    NUMSAMPLES = 81
    SAMPLINGFRACTION = 82
    SAMPLECOUNTS = 83
    SYNOPSIS = 101

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.__repr__()
