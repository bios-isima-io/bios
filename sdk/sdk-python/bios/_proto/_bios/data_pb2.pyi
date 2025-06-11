from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

AVG: MetricFunction
BLOB: AttributeType
BOOLEAN: AttributeType
COUNT: MetricFunction
CSV: ContentRepresentation
DCLB1: MetricFunction
DCLB2: MetricFunction
DCLB3: MetricFunction
DCUB1: MetricFunction
DCUB2: MetricFunction
DCUB3: MetricFunction
DECIMAL: AttributeType
DESCRIPTOR: _descriptor.FileDescriptor
DISTINCTCOUNT: MetricFunction
GLOBAL_WINDOW: WindowType
INTEGER: AttributeType
KURTOSIS: MetricFunction
LAST: MetricFunction
MAX: MetricFunction
MEDIAN: MetricFunction
MIN: MetricFunction
NATIVE: ContentRepresentation
NUMSAMPLES: MetricFunction
P0_01: MetricFunction
P0_1: MetricFunction
P1: MetricFunction
P10: MetricFunction
P25: MetricFunction
P50: MetricFunction
P75: MetricFunction
P90: MetricFunction
P99: MetricFunction
P99_9: MetricFunction
P99_99: MetricFunction
SAMPLECOUNTS: MetricFunction
SAMPLINGFRACTION: MetricFunction
SKEWNESS: MetricFunction
SLIDING_WINDOW: WindowType
STDDEV: MetricFunction
STRING: AttributeType
SUM: MetricFunction
SUM2: MetricFunction
SUM3: MetricFunction
SUM4: MetricFunction
SYNOPSIS: MetricFunction
TUMBLING_WINDOW: WindowType
UNKNOWN: AttributeType
UNTYPED: ContentRepresentation
VARIANCE: MetricFunction

class AttributeList(_message.Message):
    __slots__ = ["attributes"]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    attributes: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, attributes: _Optional[_Iterable[str]] = ...) -> None: ...

class ColumnDefinition(_message.Message):
    __slots__ = ["index_in_value_array", "name", "type"]
    INDEX_IN_VALUE_ARRAY_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    index_in_value_array: int
    name: str
    type: AttributeType
    def __init__(self, type: _Optional[_Union[AttributeType, str]] = ..., name: _Optional[str] = ..., index_in_value_array: _Optional[int] = ...) -> None: ...

class Dimensions(_message.Message):
    __slots__ = ["dimensions"]
    DIMENSIONS_FIELD_NUMBER: _ClassVar[int]
    dimensions: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, dimensions: _Optional[_Iterable[str]] = ...) -> None: ...

class InsertBulkErrorResponse(_message.Message):
    __slots__ = ["results_with_error", "server_error_code", "server_error_message"]
    RESULTS_WITH_ERROR_FIELD_NUMBER: _ClassVar[int]
    SERVER_ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    SERVER_ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    results_with_error: _containers.RepeatedCompositeFieldContainer[InsertSuccessOrError]
    server_error_code: str
    server_error_message: str
    def __init__(self, server_error_code: _Optional[str] = ..., server_error_message: _Optional[str] = ..., results_with_error: _Optional[_Iterable[_Union[InsertSuccessOrError, _Mapping]]] = ...) -> None: ...

class InsertBulkRequest(_message.Message):
    __slots__ = ["content_rep", "record", "schema_version", "signal"]
    CONTENT_REP_FIELD_NUMBER: _ClassVar[int]
    RECORD_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    SIGNAL_FIELD_NUMBER: _ClassVar[int]
    content_rep: ContentRepresentation
    record: _containers.RepeatedCompositeFieldContainer[Record]
    schema_version: int
    signal: str
    def __init__(self, content_rep: _Optional[_Union[ContentRepresentation, str]] = ..., record: _Optional[_Iterable[_Union[Record, _Mapping]]] = ..., signal: _Optional[str] = ..., schema_version: _Optional[int] = ...) -> None: ...

class InsertBulkSuccessResponse(_message.Message):
    __slots__ = ["responses"]
    RESPONSES_FIELD_NUMBER: _ClassVar[int]
    responses: _containers.RepeatedCompositeFieldContainer[InsertSuccessResponse]
    def __init__(self, responses: _Optional[_Iterable[_Union[InsertSuccessResponse, _Mapping]]] = ...) -> None: ...

class InsertRequest(_message.Message):
    __slots__ = ["content_rep", "record", "schema_version"]
    CONTENT_REP_FIELD_NUMBER: _ClassVar[int]
    RECORD_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    content_rep: ContentRepresentation
    record: Record
    schema_version: int
    def __init__(self, content_rep: _Optional[_Union[ContentRepresentation, str]] = ..., record: _Optional[_Union[Record, _Mapping]] = ..., schema_version: _Optional[int] = ...) -> None: ...

class InsertSuccessOrError(_message.Message):
    __slots__ = ["error_message", "event_id", "insert_timestamp", "server_error_code"]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    INSERT_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    SERVER_ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    error_message: str
    event_id: str
    insert_timestamp: int
    server_error_code: str
    def __init__(self, event_id: _Optional[str] = ..., insert_timestamp: _Optional[int] = ..., error_message: _Optional[str] = ..., server_error_code: _Optional[str] = ...) -> None: ...

class InsertSuccessResponse(_message.Message):
    __slots__ = ["event_id", "insert_timestamp"]
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    INSERT_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    event_id: bytes
    insert_timestamp: int
    def __init__(self, event_id: _Optional[bytes] = ..., insert_timestamp: _Optional[int] = ...) -> None: ...

class Metric(_message.Message):
    __slots__ = ["function", "name", "of"]
    AS_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    OF_FIELD_NUMBER: _ClassVar[int]
    function: MetricFunction
    name: str
    of: str
    def __init__(self, function: _Optional[_Union[MetricFunction, str]] = ..., of: _Optional[str] = ..., name: _Optional[str] = ..., **kwargs) -> None: ...

class OrderBy(_message.Message):
    __slots__ = ["by", "case_sensitive", "reverse"]
    BY_FIELD_NUMBER: _ClassVar[int]
    CASE_SENSITIVE_FIELD_NUMBER: _ClassVar[int]
    REVERSE_FIELD_NUMBER: _ClassVar[int]
    by: str
    case_sensitive: bool
    reverse: bool
    def __init__(self, by: _Optional[str] = ..., reverse: bool = ..., case_sensitive: bool = ...) -> None: ...

class QueryResult(_message.Message):
    __slots__ = ["records", "window_begin_time"]
    RECORDS_FIELD_NUMBER: _ClassVar[int]
    WINDOW_BEGIN_TIME_FIELD_NUMBER: _ClassVar[int]
    records: _containers.RepeatedCompositeFieldContainer[Record]
    window_begin_time: int
    def __init__(self, window_begin_time: _Optional[int] = ..., records: _Optional[_Iterable[_Union[Record, _Mapping]]] = ...) -> None: ...

class Record(_message.Message):
    __slots__ = ["blob_values", "boolean_values", "double_values", "event_id", "long_values", "string_values", "timestamp"]
    BLOB_VALUES_FIELD_NUMBER: _ClassVar[int]
    BOOLEAN_VALUES_FIELD_NUMBER: _ClassVar[int]
    DOUBLE_VALUES_FIELD_NUMBER: _ClassVar[int]
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    LONG_VALUES_FIELD_NUMBER: _ClassVar[int]
    STRING_VALUES_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    blob_values: _containers.RepeatedScalarFieldContainer[bytes]
    boolean_values: _containers.RepeatedScalarFieldContainer[bool]
    double_values: _containers.RepeatedScalarFieldContainer[float]
    event_id: bytes
    long_values: _containers.RepeatedScalarFieldContainer[int]
    string_values: _containers.RepeatedScalarFieldContainer[str]
    timestamp: int
    def __init__(self, event_id: _Optional[bytes] = ..., timestamp: _Optional[int] = ..., long_values: _Optional[_Iterable[int]] = ..., double_values: _Optional[_Iterable[float]] = ..., string_values: _Optional[_Iterable[str]] = ..., blob_values: _Optional[_Iterable[bytes]] = ..., boolean_values: _Optional[_Iterable[bool]] = ...) -> None: ...

class SelectQuery(_message.Message):
    __slots__ = ["attributes", "distinct", "end_time", "group_by", "limit", "metrics", "on_the_fly", "order_by", "start_time", "where", "windows"]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    DISTINCT_FIELD_NUMBER: _ClassVar[int]
    END_TIME_FIELD_NUMBER: _ClassVar[int]
    FROM_FIELD_NUMBER: _ClassVar[int]
    GROUP_BY_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    METRICS_FIELD_NUMBER: _ClassVar[int]
    ON_THE_FLY_FIELD_NUMBER: _ClassVar[int]
    ORDER_BY_FIELD_NUMBER: _ClassVar[int]
    START_TIME_FIELD_NUMBER: _ClassVar[int]
    WHERE_FIELD_NUMBER: _ClassVar[int]
    WINDOWS_FIELD_NUMBER: _ClassVar[int]
    attributes: AttributeList
    distinct: bool
    end_time: int
    group_by: Dimensions
    limit: int
    metrics: _containers.RepeatedCompositeFieldContainer[Metric]
    on_the_fly: bool
    order_by: OrderBy
    start_time: int
    where: str
    windows: _containers.RepeatedCompositeFieldContainer[Window]
    def __init__(self, start_time: _Optional[int] = ..., end_time: _Optional[int] = ..., distinct: bool = ..., attributes: _Optional[_Union[AttributeList, _Mapping]] = ..., metrics: _Optional[_Iterable[_Union[Metric, _Mapping]]] = ..., where: _Optional[str] = ..., group_by: _Optional[_Union[Dimensions, _Mapping]] = ..., windows: _Optional[_Iterable[_Union[Window, _Mapping]]] = ..., order_by: _Optional[_Union[OrderBy, _Mapping]] = ..., limit: _Optional[int] = ..., on_the_fly: bool = ..., **kwargs) -> None: ...

class SelectQueryResponse(_message.Message):
    __slots__ = ["data", "definitions", "is_windowed_response", "request_query_num"]
    DATA_FIELD_NUMBER: _ClassVar[int]
    DEFINITIONS_FIELD_NUMBER: _ClassVar[int]
    IS_WINDOWED_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    REQUEST_QUERY_NUM_FIELD_NUMBER: _ClassVar[int]
    data: _containers.RepeatedCompositeFieldContainer[QueryResult]
    definitions: _containers.RepeatedCompositeFieldContainer[ColumnDefinition]
    is_windowed_response: bool
    request_query_num: int
    def __init__(self, data: _Optional[_Iterable[_Union[QueryResult, _Mapping]]] = ..., definitions: _Optional[_Iterable[_Union[ColumnDefinition, _Mapping]]] = ..., is_windowed_response: bool = ..., request_query_num: _Optional[int] = ...) -> None: ...

class SelectRequest(_message.Message):
    __slots__ = ["queries"]
    QUERIES_FIELD_NUMBER: _ClassVar[int]
    queries: _containers.RepeatedCompositeFieldContainer[SelectQuery]
    def __init__(self, queries: _Optional[_Iterable[_Union[SelectQuery, _Mapping]]] = ...) -> None: ...

class SelectResponse(_message.Message):
    __slots__ = ["responses"]
    RESPONSES_FIELD_NUMBER: _ClassVar[int]
    responses: _containers.RepeatedCompositeFieldContainer[SelectQueryResponse]
    def __init__(self, responses: _Optional[_Iterable[_Union[SelectQueryResponse, _Mapping]]] = ...) -> None: ...

class SlidingWindow(_message.Message):
    __slots__ = ["slide_interval", "window_slides"]
    SLIDE_INTERVAL_FIELD_NUMBER: _ClassVar[int]
    WINDOW_SLIDES_FIELD_NUMBER: _ClassVar[int]
    slide_interval: int
    window_slides: int
    def __init__(self, slide_interval: _Optional[int] = ..., window_slides: _Optional[int] = ...) -> None: ...

class TumblingWindow(_message.Message):
    __slots__ = ["window_size_ms"]
    WINDOW_SIZE_MS_FIELD_NUMBER: _ClassVar[int]
    window_size_ms: int
    def __init__(self, window_size_ms: _Optional[int] = ...) -> None: ...

class Uuid(_message.Message):
    __slots__ = ["hi", "lo"]
    HI_FIELD_NUMBER: _ClassVar[int]
    LO_FIELD_NUMBER: _ClassVar[int]
    hi: int
    lo: int
    def __init__(self, hi: _Optional[int] = ..., lo: _Optional[int] = ...) -> None: ...

class Window(_message.Message):
    __slots__ = ["sliding", "tumbling", "window_type"]
    SLIDING_FIELD_NUMBER: _ClassVar[int]
    TUMBLING_FIELD_NUMBER: _ClassVar[int]
    WINDOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    sliding: SlidingWindow
    tumbling: TumblingWindow
    window_type: WindowType
    def __init__(self, window_type: _Optional[_Union[WindowType, str]] = ..., sliding: _Optional[_Union[SlidingWindow, _Mapping]] = ..., tumbling: _Optional[_Union[TumblingWindow, _Mapping]] = ...) -> None: ...

class MetricFunction(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []

class ContentRepresentation(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []

class AttributeType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []

class WindowType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
