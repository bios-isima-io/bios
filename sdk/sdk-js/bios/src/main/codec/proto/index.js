import Long from 'long';
import protobuf from 'protobufjs';

import { com } from './biosProtoMessages';

// insert request
export const ContentRepresentation = com.isima.bios.models.proto.ContentRepresentation;
export const InsertBulkRequest = com.isima.bios.models.proto.InsertBulkRequest;
export const InsertSuccessResponse = com.isima.bios.models.proto.InsertSuccessResponse;
export const InsertRequest = com.isima.bios.models.proto.InsertRequest;
export const Uuid = com.isima.bios.models.proto.Uuid;

// insert response
export const InsertBulkResponse = com.isima.bios.models.proto.InsertBulkSuccessResponse;
export const InsertResponse = com.isima.bios.models.proto.InsertSuccessResponse;
export const InsertBulkErrorResponse = com.isima.bios.models.proto.InsertBulkErrorResponse;
export const InsertSuccessOrError = com.isima.bios.models.proto.InsertSuccessorError;

// select request
export const Dimensions = com.isima.bios.models.proto.Dimensions;
export const Metric = com.isima.bios.models.proto.Metric;
export const OrderBy = com.isima.bios.models.proto.OrderBy;
export const QueryResult = com.isima.bios.models.proto.QueryResult;
export const SelectRequest = com.isima.bios.models.proto.SelectRequest;
export const SelectQuery = com.isima.bios.models.proto.SelectQuery;
export const Window = com.isima.bios.models.proto.Window;

// select response
export const AttributeList = com.isima.bios.models.proto.AttributeList;
export const ColumnDefinition = com.isima.bios.models.proto.ColumnDefinition;
export const SelectResponse = com.isima.bios.models.proto.SelectResponse;
export const Record = com.isima.bios.models.proto.Record;

// enums
export const AttributeOrigin = com.isima.bios.models.proto.AttributeOrigin;
export const AttributeType = com.isima.bios.models.proto.AttributeType;
export const MetricFunction = com.isima.bios.models.proto.MetricFunction;
export const WindowType = com.isima.bios.models.proto.WindowType;

// This is required for proper 64-bit integer support.
// protobuf.util.Long = Long;
// protobuf.configure();
