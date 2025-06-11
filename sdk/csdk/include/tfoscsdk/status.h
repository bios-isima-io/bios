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
#ifndef TFOSCSDK_STATUSES_H_
#define TFOSCSDK_STATUSES_H_

#include <string>

#include "tfoscsdk/types.h"

namespace tfos::csdk {

enum class OpStatus : unsigned int {
  OK,
  GENERIC_CLIENT_ERROR,
  INVALID_ARGUMENT,
  SESSION_INACTIVE,
  PARSER_ERROR,
  CLIENT_ALREADY_STARTED,
  REQUEST_TOO_LARGE,
  INVALID_STATE,
  CLIENT_CHANNEL_ERROR,
  SERVER_CONNECTION_FAILURE,
  SERVER_CHANNEL_ERROR,
  UNAUTHORIZED,
  FORBIDDEN,
  SERVICE_UNAVAILABLE,
  GENERIC_SERVER_ERROR,
  TIMEOUT,
  BAD_GATEWAY,
  SERVICE_UNDEPLOYED,
  OPERATION_CANCELLED,
  OPERATION_UNEXECUTABLE,
  SESSION_EXPIRED,
  NO_SUCH_TENANT,
  NO_SUCH_STREAM,
  NOT_FOUND,
  TENANT_ALREADY_EXISTS,
  STREAM_ALREADY_EXISTS,
  RESOURCE_ALREADY_EXISTS,
  BAD_INPUT,
  NOT_IMPLEMENTED,
  CONSTRAINT_WARNING,
  SCHEMA_VERSION_CHANGED,
  INVALID_REQUEST,
  BULK_INGEST_FAILED,
  SERVER_DATA_ERROR,
  SCHEMA_MISMATCHED,
  USER_ID_NOT_VERIFIED,
  END  // This entry must be the last.
};

extern std::ostream &operator<<(std::ostream &os, OpStatus status);

#define SERVER_CODE_CONSTRAINT_WARNING "GENERIC05"
#define SERVER_CODE_OPERATION_UNEXECUTABLE "GENERIC0b"
#define SERVER_CODE_NO_SUCH_TENANT "ADMIN03"
#define SERVER_CODE_NO_SUCH_STREAM "ADMIN04"
#define SERVER_CODE_TENANT_ALREADY_EXISTS "ADMIN08"
#define SERVER_CODE_STREAM_ALREADY_EXISTS "ADMIN09"
#define SERVER_CODE_STREAM_VERSION_MISMATCH "ADMIN14"
#define SERVER_CODE_SESSION_EXPIRED "AUTH0a"
#define SERVER_CODE_USER_ID_NOT_VERIFIED "AUTH11"
#define SERVER_CODE_TOO_MANY_VALUES "INGEST03"
#define SERVER_CODE_TOO_FEW_VALUES "INGEST04"

/**
 * Utility class to manipulate OpStatus enum.
 */
class Statuses final {
 public:
  /**
   * Returns the name of a OpStatus entry.
   * @param status A OpStatus entry.
   * @returns The OpStatus name.
   */
  static std::string Name(OpStatus status);
  static string_t CName(OpStatus status);

  /**
   * Returns the status code of a OpStatus entry.
   * @param status OpStatus entry.
   * @returns The status code of the OpStatus entry.
   */
  static status_code_t StatusCode(OpStatus status);

  /**
   * Resolves OpStatus entry by status code.
   * @param status_code Status code.
   * @returns Resolved OpStatus entry if resolved, otherwise OpStatus::END.
   */
  static OpStatus FromStatusCode(status_code_t status_code);

  /**
   * Returns the description of a OpStatus entry.
   * @param status A OpStatus entry.
   * @returns The OpStatus description.
   */
  static std::string Description(OpStatus status);
};
}  // namespace tfos::csdk

#endif  // TFOSCSDK_STATUSES_H_
