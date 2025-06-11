/*
 * Copyright (C) 2025 Isima, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "tfoscsdk/status.h"

#include <assert.h>
#include <string.h>

namespace tfos::csdk {

struct StatusEntry {
  OpStatus status;
  const char *name;
  status_code_t code;
  std::string description;
};

static const StatusEntry kStatuses[] = {
    {OpStatus::OK, "OK", 0x000000, "Successful"},
    {OpStatus::GENERIC_CLIENT_ERROR, "GENERIC_CLIENT_ERROR", 0x100001,
     "An unexpected error occurred on the clien"},
    {OpStatus::INVALID_ARGUMENT, "INVALID_ARGUMENT", 0x100002,
     "CommonError while validating arguments passed to the API"},
    {OpStatus::SESSION_INACTIVE, "SESSION_INACTIVE", 0x100003,
     "A request for an operation was issued to the session, but it is closed "
     "already"},
    {OpStatus::PARSER_ERROR, "PARSER_ERROR", 0x100004,
     "Failed to convert a string to a TFOS request object"},
    {OpStatus::CLIENT_ALREADY_STARTED, "CLIENT_ALREADY_STARTED", 0x100005,
     "Session has already started"},
    {OpStatus::REQUEST_TOO_LARGE, "REQUEST_TOO_LARGE", 0x100006, "Request entity too large"},
    {OpStatus::INVALID_STATE, "INVALID_STATE", 0x100007, "Client is in invalid state"},
    {OpStatus::CLIENT_CHANNEL_ERROR, "CLIENT_CHANNEL_ERROR", 0x100008,
     "SDK failed to communicate with peer for an unexpected reason"},
    {OpStatus::SERVER_CONNECTION_FAILURE, "SERVER_CONNECTION_FAILURE", 0x200001,
     "SDK failed to connect server"},
    {OpStatus::SERVER_CHANNEL_ERROR, "SERVER_CHANNEL_ERROR", 0x200002,
     "SDK encountered a communication error"},
    {OpStatus::UNAUTHORIZED, "UNAUTHORIZED", 0x200003, "Not authorized"},
    {OpStatus::FORBIDDEN, "FORBIDDEN", 0x200004, "Permission denied"},
    {OpStatus::SERVICE_UNAVAILABLE, "SERVICE_UNAVAILABLE", 0x200005,
     "Server is currently unavailable"},
    {OpStatus::GENERIC_SERVER_ERROR, "GENERIC_SERVER_ERROR", 0x200006,
     "An unexpected problem happened on server side"},
    {OpStatus::TIMEOUT, "TIMEOUT", 0x200007, "Operation has timed out"},
    {OpStatus::BAD_GATEWAY, "BAD_GATEWAY", 0x200008,
     "Load balancer proxy failed due to upstream server crash"},
    {OpStatus::SERVICE_UNDEPLOYED, "SERVICE_UNDEPLOYED", 0x200009,
     "Requested resource was not found on the server"},
    {OpStatus::OPERATION_CANCELLED, "OPERATION_CANCELLED", 0x20000a,
     "Operation has been cancelled by peer"},
    {OpStatus::OPERATION_UNEXECUTABLE, "OPERATION_UNEXECUTABLE", 0x20000b,
     "Unable to execute the requested operation"},
    {OpStatus::SESSION_EXPIRED, "SESSION_EXPIRED", 0x20000c,
     "Session expired. Please close this session and start another one"},
    {OpStatus::NO_SUCH_TENANT, "NO_SUCH_TENANT", 0x210001,
     "SDK tried an operation that requires a tenant, but the specified tenant "
     "does not exist"},
    {OpStatus::NO_SUCH_STREAM, "NO_SUCH_STREAM", 0x210002,
     "SDK tried an operation that requires a stream, but the specified stream "
     "does not exist"},
    {OpStatus::NOT_FOUND, "NOT_FOUND", 0x210003, "Target entity does not exist in server"},
    {OpStatus::TENANT_ALREADY_EXISTS, "TENANT_ALREADY_EXISTS", 0x210004,
     "SDK tried to add an tenant, but specified tenant already exists"},
    {OpStatus::STREAM_ALREADY_EXISTS, "STREAM_ALREADY_EXISTS", 0x210005,
     "SDK tried to add a stream, but specified stream already exists"},
    {OpStatus::RESOURCE_ALREADY_EXISTS, "RESOURCE_ALREADY_EXISTS", 0x210006,
     "SDK tried to add an entity to server, but the target already exists"},
    {OpStatus::BAD_INPUT, "BAD_INPUT", 0x210007,
     "Server rejected input due to invalid data or format"},
    {OpStatus::NOT_IMPLEMENTED, "NOT_IMPLEMENTED", 0x210008, "Not implemented"},
    {OpStatus::CONSTRAINT_WARNING, "CONSTRAINT_WARNING", 0x210009,
     "Constraint warning; Set force option to override"},
    {OpStatus::SCHEMA_VERSION_CHANGED, "SCHEMA_VERSION_CHANGED", 0x21000b,
     "Given stream version no longer exists"},
    {OpStatus::INVALID_REQUEST, "INVALID_REQUEST", 0x21000c,
     "Server rejected request due to invalid request"},
    {OpStatus::BULK_INGEST_FAILED, "BULK_INGEST_FAILED", 0x21000d, "Bulk ingest failed"},
    {OpStatus::SERVER_DATA_ERROR, "SERVER_DATA_ERROR", 0x21000e, "Server returned malformed data"},
    {OpStatus::SCHEMA_MISMATCHED, "SCHEMA_MISMATCHED", 0x21000f, "Schema mismatched"},
    {OpStatus::USER_ID_NOT_VERIFIED, "USER_ID_NOT_VERIFIED", 0x210010,
     "User ID is not verified yet"}};

static_assert(sizeof(kStatuses) / sizeof(StatusEntry) == static_cast<uint32_t>(OpStatus::END),
              "Size of array kStatuses must be equal to number of ClientError entries");

std::ostream &operator<<(std::ostream &os, OpStatus status) {
  os << Statuses::Name(status) << std::string(" (") << std::to_string(Statuses::StatusCode(status))
     << std::string(")");
  return os;
}

/**
 * Utility to get ordinal of a ClientError element.
 */
static inline uint32_t Ordinal(OpStatus error) { return static_cast<uint32_t>(error); }

std::string Statuses::Name(OpStatus status) { return kStatuses[Ordinal(status)].name; }

string_t Statuses::CName(OpStatus status) {
  const char *cstr = kStatuses[Ordinal(status)].name;
  return {.text = cstr, .length = static_cast<int32_t>(strlen(cstr))};
}

status_code_t Statuses::StatusCode(OpStatus error) { return kStatuses[Ordinal(error)].code; }

OpStatus Statuses::FromStatusCode(status_code_t status_code) {
  for (int i = 0; kStatuses[i].status != OpStatus::END; ++i) {
    if (kStatuses[i].code == status_code) {
      return kStatuses[i].status;
    }
  }
  return OpStatus::END;
}

std::string Statuses::Description(OpStatus error) { return kStatuses[Ordinal(error)].description; }

}  // namespace tfos::csdk
