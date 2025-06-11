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
import math
from typing import List, Union

from ._proto._bios import data_pb2
from ._utils import check_not_empty_string, check_str_list, check_type
from .errors import ErrorCode, ServiceError
from .models import Metric, MetricFunction
from .models.isql_request_message import ISqlRequestMessage, ISqlRequestType


class SelectContext:
    """Bios SelectContext class"""

    _FUNCTION_B2P = {}
    _FUNCTION_B2P[MetricFunction.SUM] = data_pb2.MetricFunction.SUM
    _FUNCTION_B2P[MetricFunction.COUNT] = data_pb2.MetricFunction.COUNT
    _FUNCTION_B2P[MetricFunction.MIN] = data_pb2.MetricFunction.MIN
    _FUNCTION_B2P[MetricFunction.MAX] = data_pb2.MetricFunction.MAX
    _FUNCTION_B2P[MetricFunction.LAST] = data_pb2.MetricFunction.LAST
    _FUNCTION_B2P[MetricFunction.AVG] = data_pb2.MetricFunction.AVG
    _FUNCTION_B2P[MetricFunction.VARIANCE] = data_pb2.MetricFunction.VARIANCE
    _FUNCTION_B2P[MetricFunction.STDDEV] = data_pb2.MetricFunction.STDDEV
    _FUNCTION_B2P[MetricFunction.SKEWNESS] = data_pb2.MetricFunction.SKEWNESS
    _FUNCTION_B2P[MetricFunction.KURTOSIS] = data_pb2.MetricFunction.KURTOSIS
    _FUNCTION_B2P[MetricFunction.SUM2] = data_pb2.MetricFunction.SUM2
    _FUNCTION_B2P[MetricFunction.SUM3] = data_pb2.MetricFunction.SUM3
    _FUNCTION_B2P[MetricFunction.SUM4] = data_pb2.MetricFunction.SUM4
    _FUNCTION_B2P[MetricFunction.MEDIAN] = data_pb2.MetricFunction.MEDIAN
    _FUNCTION_B2P[MetricFunction.P0_01] = data_pb2.MetricFunction.P0_01
    _FUNCTION_B2P[MetricFunction.P0_1] = data_pb2.MetricFunction.P0_1
    _FUNCTION_B2P[MetricFunction.P1] = data_pb2.MetricFunction.P1
    _FUNCTION_B2P[MetricFunction.P10] = data_pb2.MetricFunction.P10
    _FUNCTION_B2P[MetricFunction.P25] = data_pb2.MetricFunction.P25
    _FUNCTION_B2P[MetricFunction.P50] = data_pb2.MetricFunction.P50
    _FUNCTION_B2P[MetricFunction.P75] = data_pb2.MetricFunction.P75
    _FUNCTION_B2P[MetricFunction.P90] = data_pb2.MetricFunction.P90
    _FUNCTION_B2P[MetricFunction.P99] = data_pb2.MetricFunction.P99
    _FUNCTION_B2P[MetricFunction.P99_9] = data_pb2.MetricFunction.P99_9
    _FUNCTION_B2P[MetricFunction.P99_99] = data_pb2.MetricFunction.P99_99
    _FUNCTION_B2P[MetricFunction.DISTINCTCOUNT] = data_pb2.MetricFunction.DISTINCTCOUNT
    _FUNCTION_B2P[MetricFunction.DCLB1] = data_pb2.MetricFunction.DCLB1
    _FUNCTION_B2P[MetricFunction.DCUB1] = data_pb2.MetricFunction.DCUB1
    _FUNCTION_B2P[MetricFunction.DCLB2] = data_pb2.MetricFunction.DCLB2
    _FUNCTION_B2P[MetricFunction.DCUB2] = data_pb2.MetricFunction.DCUB2
    _FUNCTION_B2P[MetricFunction.DCLB3] = data_pb2.MetricFunction.DCLB3
    _FUNCTION_B2P[MetricFunction.DCUB3] = data_pb2.MetricFunction.DCUB3
    _FUNCTION_B2P[MetricFunction.NUMSAMPLES] = data_pb2.MetricFunction.NUMSAMPLES
    _FUNCTION_B2P[MetricFunction.SAMPLINGFRACTION] = data_pb2.MetricFunction.SAMPLINGFRACTION
    _FUNCTION_B2P[MetricFunction.SAMPLECOUNTS] = data_pb2.MetricFunction.SAMPLECOUNTS
    _FUNCTION_B2P[MetricFunction.SYNOPSIS] = data_pb2.MetricFunction.SYNOPSIS

    def __init__(self, *select_targets: Union[str, Metric]):
        """Select metrics to retrieve.

        Args:
            select_targets (array of str or Metric): metrics to select.
                A value can be a name of attribute, a string expression of metric
                e.g. "sum(clicks)", or an instance of class Metric.
        """
        self.query = data_pb2.SelectQuery()
        attributes = None
        self.attributes = []
        self.metrics = []
        for metric_spec in select_targets:
            if not attributes:
                attributes = data_pb2.AttributeList()
            validated = self._validate_metric_spec(metric_spec)
            if validated.name:
                attributes.attributes.append(validated.name)
                self.attributes.append(validated.name)
            elif validated.function:
                metric = self.query.metrics.add()
                metric.function = self._convert_function(validated.function)
                if validated.of:
                    metric.of = validated.of
                if validated.get_as():
                    setattr(metric, "as", validated.get_as())
                self.metrics.append(
                    {
                        "function": validated.function.name,
                        "of": validated.of if validated.of else None,
                        "as": validated.get_as() if validated.get_as() else None,
                    }
                )

        if attributes is not None:
            self.query.attributes.CopyFrom(attributes)

        # Used by snapped time window, start time and end time are calculated on building
        self.origin = None
        self.delta = None
        self.interval = None

    def _validate_metric_spec(self, metric_spec):
        if isinstance(metric_spec, Metric):
            return metric_spec

        if isinstance(metric_spec, str):
            return Metric(metric_spec)

        raise ServiceError(
            ErrorCode.INVALID_ARGUMENT,
            "select() method parameter must be of type Metric or string",
        )

    def _convert_function(self, function):
        return self._FUNCTION_B2P.get(function)


class ISqlSelectFromContext(ISqlRequestMessage):
    """Bios ISqlSelectFromContext class.
    This class is required to select context entries. Its built using ISqlRequest
    to execute the select, use  bios session.execute function.
    """

    def __init__(self, request):
        super().__init__(ISqlRequestType.SELECT_CONTEXT)
        self.keys = request.keys
        self.name = request.name
        self.attr = request.attr
        self.on_the_fly = request.enable_on_the_fly

    def __repr__(self):
        json_dict = {
            "contentRepresentation": "UNTYPED",
            "primaryKeys": self.keys,
        }
        if self.attr is not None:
            json_dict["attributes"] = [",".join(self.attr)]
        if self.on_the_fly:
            json_dict["onTheFly"] = True

        return json.dumps(json_dict)


class SelectFromContextSerializable(dict):
    """Class used by MultiGetContextEntriesRequest below to enable nested json encoding."""

    def __init__(self, primary_keys, attributes):
        if attributes is not None:
            dict.__init__(
                self,
                primaryKeys=primary_keys,
                attributes=attributes,
                contentRepresentation="UNTYPED",
            )
        else:
            dict.__init__(self, primaryKeys=primary_keys, contentRepresentation="UNTYPED")


class MultiGetContextEntriesRequest:
    """Non-iSQL class used to create a network message for a multi-get request."""

    def __init__(self, context_names: list[str], queries: list[ISqlSelectFromContext]):
        self.context_names = context_names
        self.queries = [SelectFromContextSerializable(q.keys, q.attr) for q in queries]

    def __repr__(self):
        json_dict = {
            "contextNames": self.context_names,
            "queries": self.queries,
        }
        json_string = json.dumps(json_dict)
        return json_string


class LimitEx:
    """Bios LimitEx class"""

    def __init__(self, _query):
        """Limits the number of results"""
        self._query = _query

    def limit(self, limit: int):
        check_type(limit, "limitation", int)
        self._query["limit"] = limit
        return self


class GetContextEntriesRequestBuilder:
    """Query builder specifically used for building query by primary keys."""

    def __init__(self, name: str, keys: List[List[str]] = None, attr: List[str] = None):
        self.name = name
        self.keys = keys
        self.attr = attr if attr else None
        self.enable_on_the_fly = False

    def on_the_fly(self):
        """Enable recency fetch policy if the target context is a materialized context of a
        signal"""
        self.enable_on_the_fly = True
        return self

    def build(self):
        return ISqlSelectFromContext(self)


class SelectFromContextRequestEx(LimitEx):
    """Bios SelectFromContextRequestEx class"""

    def __init__(self, context=None, filtering=None, keys=None, attributes=None, metrics=None):
        self._query = {}
        check_not_empty_string(context, "context")
        if filtering is not None:
            check_not_empty_string(filtering, "filtering")
        self._query["attributes"] = attributes
        self._query["metrics"] = metrics
        self._query["context"] = context.strip()
        self._query["where"] = filtering
        self._name = None
        self._keys = keys
        self._attr = None
        self._metric = []
        super().__init__(self._query)

    def order_by(
        self,
        sort_spec: str,
        reverse: bool = False,
        case_sensitive: bool = False,
    ):
        """Specifies the attribute to be used for sorting the result.

           Returned events would be sorted by timestamp when :timestamp is specified
           to the sort_spec.

        Args:
            sort_spec (str): sort key name
            reverse(bool): reverse the order
        """
        check_not_empty_string(sort_spec, "sort_spec")
        order_by = {}
        order_by["by"] = sort_spec
        order_by["reverse"] = reverse
        order_by["caseSensitive"] = case_sensitive
        self._query["orderBy"] = order_by
        return self

    def group_by(self, *group_by: Union[str, List[str]]):
        dimensions = []
        for i, element in enumerate(group_by):
            if isinstance(element, list):
                check_str_list(element, "group_by")
                dimensions.extend(list(element))
            else:
                check_not_empty_string(element, f"group_by[{i}]")
                dimensions.append(element)
        self._query["groupBy"] = dimensions
        return self

    def on_the_fly(self):
        """Specifies that the latest changes to the context entries should also be
        considered, not just the changes already indexed.
        """
        self._query["onTheFly"] = True
        return self

    def build(self):
        """Builds the select request message"""
        get_context_entry_request = self._try_get_context_entries_request()
        if get_context_entry_request:
            return get_context_entry_request
        return ISqlSelectRequestMessageBuilderEx(self._query)

    def _try_get_context_entries_request(self):
        if self._query.get("where") or self._query.get("groupBy"):
            return None

        if not self._metric and not self._attr:
            raise ServiceError(
                ErrorCode.INVALID_REQUEST,
                "Context select must specify either metric column(s) or where clause",
            )

        if self._metric:
            return None
        return GetContextEntriesRequestBuilder(
            self._name, keys=self._keys, attr=self._attr
        ).build()


class SelectFromContextRequest(SelectFromContextRequestEx):
    """isql Class to select from a context
    Keeps the name of the context and provides where method to filter selection
    """

    def __init__(self, name: str, attr=None, metric=None, attributes_str=None, metrics=None):
        super().__init__(context=name, attributes=attributes_str, metrics=metrics)
        self._name = name
        self._attr = attr
        self._metric = metric
        self._metrics = metrics

    def where(self, where: str = None, keys: List[List[str]] = None) -> SelectFromContextRequestEx:
        """Method to specify filters to select context
        Args:
            keys: List of keys - each key is a list of attributes
                  or a filtering string
        Returns:
            SelectFromContextRequest or SelectFromContextRequestEx
        """
        if isinstance(keys, list):
            return GetContextEntriesRequestBuilder(self._name, keys=keys, attr=self._attr)
        if where is not None:
            check_not_empty_string(where, "where")
        return SelectFromContextRequestEx(
            context=self._name,
            filtering=where,
            attributes=self._query["attributes"],
            metrics=self._query["metrics"],
        )


class ISqlSelectRequestMessageBuilderEx(ISqlRequestMessage):
    """Bios ISqlSelectRequestMessageBuilderEx class"""

    def __init__(self, query):
        super().__init__(ISqlRequestType.SELECT_CONTEXT_EX)
        self.query = query


class From:
    """Bios select Request From class"""

    def __init__(self, context):
        self._context = context

    def from_signal(self, signal: str):
        """Specifies the signal to select from

        Args:
            signal (str): The signal name to select
        """
        check_not_empty_string(signal, "signal")
        setattr(self._context.query, "from", signal.strip())
        return Where(self._context)

    def from_context(self, name: str) -> SelectFromContextRequest:
        """Specifies the context to select from
        Args:
            name (str): The context name
        Returns:
            SelectFromContextRequest
        """
        check_not_empty_string(name, "name")
        return SelectFromContextRequest(
            name,
            attr=self._context.query.attributes.attributes,
            metric=self._context.query.metrics,
            attributes_str=self._context.attributes,
            metrics=self._context.metrics,
        )


class AnyTimeRange:
    """Bios select request AnyTimeRange class"""

    def __init__(self, context):
        self._context = context

    def snapped_time_range(self, origin: int, delta: int, snap_step_size: int = None):
        """Deprecated. Use time_range() instead - the parameters are the same."""
        return self.time_range(origin, delta, snap_step_size)

    def time_range(self, origin: int, delta: int, snap_step_size: int = None):
        """Specifies the time range of the query.

        Args:
            origin (int): Origin time of the range as milliseconds since Epoch.
            delta (int): Delta time of the range in milliseconds. The value can be negative,
               in which case the start time of the range is (origin + delta) to origin
               (the origin time is at the end of the range).
            snap_step_size (int): Optional parameter to determine time granularity to "snap" the
               origin and delta times to.
               If omitted and there is a window present, the window size is used.
               If omitted and there is no window present, the origin and delta times are used
               verbatim.
        """
        check_type(origin, "origin", int)
        check_type(delta, "delta", int)
        interval = None
        if snap_step_size is not None:
            check_type(snap_step_size, "snap_step_size", int)
            interval = snap_step_size
        elif self._context.query.windows and len(self._context.query.windows) > 0:
            window = self._context.query.windows[0]
            window_type = window.window_type
            if window_type == data_pb2.WindowType.TUMBLING_WINDOW:
                interval = window.tumbling.window_size_ms
            elif window_type == data_pb2.WindowType.SLIDING_WINDOW:
                interval = window.sliding.slide_interval

        if interval:
            # Save the original values for later use.
            self._context.origin = origin
            self._context.delta = delta
            self._context.interval = interval
        else:
            # We know all the information, calculate start and end times now.
            end = origin + delta
            (start_time, end_time) = (origin, end) if origin <= end else (end, origin)
            self._context.query.start_time = start_time
            self._context.query.end_time = end_time
        return ISqlSelectRequestMessageBuilder(self._context)


class TumblingWindow(AnyTimeRange):
    """Bios select request TumblingWindow class"""

    def tumbling_window(self, window_size: int):
        """Specifies tumbling window

        Args:
            window_size (int): Tumbling window size in milliseconds
        """
        check_type(window_size, "window_size", int)
        window = self._context.query.windows.add()
        window.window_type = data_pb2.WindowType.TUMBLING_WINDOW
        window.tumbling.CopyFrom(data_pb2.TumblingWindow())
        window.tumbling.window_size_ms = window_size
        return AnyTimeRange(self._context)

    def sliding_window(self, slide_interval: int, num_slide_intervals_in_window: int):
        """Deprecated. Use hopping_window() instead.
        slide_interval -> hop_interval
        num_slide_intervals_in_window -> num_hops_in_window
        """
        return self.hopping_window(slide_interval, num_slide_intervals_in_window)

    def hopping_window(self, hop_interval: int, num_hops_in_window: int):
        """Specifies a hopping window with hop size and window size.

        Args:
            hop_interval (int): Hopping window's hop size in milliseconds
            num_hops_in_window (int): Size of hopping window as a multiple of the hop size
        """
        check_type(hop_interval, "hop_interval", int)
        check_type(num_hops_in_window, "num_hops_in_window", int)
        if num_hops_in_window < 0:
            raise ServiceError(
                ErrorCode.INVALID_REQUEST,
                "number of hops in window cannot be less than 0",
            )
        window = self._context.query.windows.add()
        window.window_type = data_pb2.WindowType.SLIDING_WINDOW
        window.sliding.CopyFrom(data_pb2.SlidingWindow())
        window.sliding.slide_interval = hop_interval
        window.sliding.window_slides = num_hops_in_window
        return AnyTimeRange(self._context)


class Limit(TumblingWindow):
    """Bios select request TumblingWindow limit class"""

    def limit(self, limitation: int):
        check_type(limitation, "limitation", int)
        self._context.query.limit = limitation
        return self


class StatementOptions(Limit):
    """Bios select request StatementOptions class"""

    def group_by(self, *dimensions: str):
        """Specifies the dimensions to be grouped by

        Args:
            dimensions (variable str): The group dimensions
        """
        dimensions_ = []
        for i, element in enumerate(dimensions):
            if isinstance(element, list):
                check_str_list(element, "dimensions")
                dimensions_.extend(list(element))
            else:
                check_not_empty_string(element, f"dimensions[{i}]")
                dimensions_.append(element)

        dimensions_proto = data_pb2.Dimensions()
        dimensions_proto.dimensions.extend(dimensions_)
        self._context.query.group_by.dimensions[:] = dimensions_proto.dimensions
        return self

    def order_by(
        self,
        sort_spec: str,
        case_sensitive: bool = False,
        reverse: bool = False,
    ):
        """Specifies the attribute to be used for sorting the result

        Args:
            sort_spec (str): sort key name
            case_sensitive(bool): make order by case sensitive
            reverse(bool): reverse the order
        """
        check_not_empty_string(sort_spec, "sort_spec")
        order_by = data_pb2.OrderBy()
        order_by.by = sort_spec
        order_by.case_sensitive = case_sensitive
        order_by.reverse = reverse
        self._context.query.order_by.CopyFrom(order_by)
        return self


class Where(StatementOptions):
    """Bios select request Where class"""

    def where(self, filter_string: str):
        """Specifies the filter to limit the results

        Args:
            filter_string (str): The filter string
        """
        check_not_empty_string(filter_string, "filter_string")
        self._context.query.where = filter_string
        return StatementOptions(self._context)


class ISqlSelectRequestMessageBuilder:
    """Bios ISqlSelectRequestMessageBuilder class"""

    def __init__(self, context):
        self._context = context

    def on_the_fly(self):
        """Specifies select on-the-fly at the last part of indexed data"""
        self._context.query.on_the_fly = True
        return ISqlSelectRequestMessageBuilder(self._context)

    def build(self):
        """Builds the select request message"""
        return ISqlSelectRequestMessage(self._context)


class ISqlSelectRequestMessage(ISqlRequestMessage):
    """Bios ISqlSelectRequestMessage class"""

    @classmethod
    def _round(cls, timestamp: int, step: int) -> int:
        """Snaps a timestamp to the beginning of the time window

        Args:
            timestamp (int): Input timestamp
            step (int): Snap segment length
        Returns: int: Snapped timestamp
        """
        return math.floor(timestamp / step) * step

    @classmethod
    def _calculate_snapped_time_range(cls, origin, delta, interval, on_the_fly):
        if delta >= 0:
            start_time = origin
            end_time = origin + delta
        else:
            start_time = origin + delta
            end_time = origin
        start_time = cls._round(start_time, interval)
        end_time = end_time if on_the_fly else cls._round(end_time, interval)
        return (start_time, end_time)

    def __init__(self, context):
        super().__init__(ISqlRequestType.SELECT)
        if context.origin and context.delta and context.interval:
            # Calculate the start and end times, taking care to snap either one or both of them.
            cls = self
            context.query.start_time, context.query.end_time = cls._calculate_snapped_time_range(
                context.origin, context.delta, context.interval, context.query.on_the_fly
            )

        self.request = context.query
