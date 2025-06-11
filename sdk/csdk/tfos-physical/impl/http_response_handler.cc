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
#include "tfos-physical/impl/http_response_handler.h"

#include <string>
#include <unordered_map>

#include "tfos-common/object_mapper.h"
#include "tfoscsdk/log.h"
#include "tfoscsdk/message_struct.h"
#include "tfoscsdk/status.h"

namespace tfos::csdk {

static const class StatusInterpreter {
 private:
  std::unordered_map<uint32_t, OpStatus> http_to_op_;

 public:
  StatusInterpreter() {
    http_to_op_[400] = OpStatus::BAD_INPUT;
    http_to_op_[401] = OpStatus::UNAUTHORIZED;
    http_to_op_[403] = OpStatus::FORBIDDEN;
    http_to_op_[404] = OpStatus::NOT_FOUND;
    http_to_op_[405] = OpStatus::SERVICE_UNDEPLOYED;
    http_to_op_[408] = OpStatus::TIMEOUT;
    http_to_op_[409] = OpStatus::RESOURCE_ALREADY_EXISTS;
    http_to_op_[413] = OpStatus::REQUEST_TOO_LARGE;
    http_to_op_[500] = OpStatus::GENERIC_SERVER_ERROR;
    http_to_op_[501] = OpStatus::NOT_IMPLEMENTED;
    http_to_op_[502] = OpStatus::BAD_GATEWAY;
    http_to_op_[503] = OpStatus::SERVICE_UNAVAILABLE;
    http_to_op_[504] = OpStatus::TIMEOUT;
  }

  OpStatus HttpToOpStatus(uint32_t http_status) const {
    auto it = http_to_op_.find(http_status);
    return it != http_to_op_.end() ? it->second : OpStatus::END;
  }

  bool IsSuccessful(uint32_t http_status) const {
    int first_digit = http_status / 100;
    return first_digit == 2 || first_digit == 3;
  }

  OpStatus DetermineError(uint32_t status_code, const std::string &server_error_code) const {
    DEBUG_LOG("DetermineError: status=%d, server_code=%s", status_code, server_error_code.c_str());
    switch (status_code) {
      case 400:
        if (server_error_code == SERVER_CODE_STREAM_VERSION_MISMATCH) {
          return OpStatus::SCHEMA_VERSION_CHANGED;
        }
        if (server_error_code == SERVER_CODE_TOO_MANY_VALUES
            || server_error_code == SERVER_CODE_TOO_FEW_VALUES) {
          return OpStatus::SCHEMA_MISMATCHED;
        }
        if (server_error_code == SERVER_CODE_CONSTRAINT_WARNING) {
          return OpStatus::CONSTRAINT_WARNING;
        }
        if (server_error_code == SERVER_CODE_OPERATION_UNEXECUTABLE) {
          return OpStatus::OPERATION_UNEXECUTABLE;
        }
        break;
      case 401:
        if (server_error_code == SERVER_CODE_SESSION_EXPIRED) {
          return OpStatus::SESSION_EXPIRED;
        }
        if (server_error_code == SERVER_CODE_USER_ID_NOT_VERIFIED) {
          return OpStatus::USER_ID_NOT_VERIFIED;
        }
        break;
      case 404:
        if (server_error_code == SERVER_CODE_NO_SUCH_TENANT) {
          return OpStatus::NO_SUCH_TENANT;
        }
        if (server_error_code == SERVER_CODE_NO_SUCH_STREAM) {
          return OpStatus::NO_SUCH_STREAM;
        }
        if (server_error_code.empty()) {
          // request not reaching to the service
          return OpStatus::SERVICE_UNDEPLOYED;
        }
        break;
      case 409: {
        if (server_error_code == SERVER_CODE_TENANT_ALREADY_EXISTS) {
          return OpStatus::TENANT_ALREADY_EXISTS;
        }
        if (server_error_code == SERVER_CODE_STREAM_ALREADY_EXISTS) {
          return OpStatus::STREAM_ALREADY_EXISTS;
        }
        break;
      }
    }
    // do generic error assignment if we reach here
    auto it = http_to_op_.find(status_code);
    if (it != http_to_op_.end()) {
      return it->second;
    }
    // unknown error, apply the generic rule
    return (status_code / 100 == 4) ? OpStatus::GENERIC_CLIENT_ERROR
                                    : OpStatus::GENERIC_SERVER_ERROR;
  }
} interpreter;

ResponseMessage HttpResponseHandler::HandleResponse(uint32_t status_code,
                                                    const std::string &content_type,
                                                    uint8_t *content, uint64_t content_length,
                                                    const std::string &endpoint,
                                                    bool bulk_response) {
  if (interpreter.IsSuccessful(status_code)) {
    return ResponseMessage(OpStatus::OK, endpoint, content, content_length);
  }

  std::string server_error_code;
  static const std::string kContentTypeJson = "application/json";
  static const std::string kContentTypeProtobuf = "application/x-protobuf";
  ObjectMapper mapper;
  if ((content_type.compare(0, kContentTypeJson.size(), kContentTypeJson) == 0) ||
      (content_type.compare(0, kContentTypeProtobuf.size(), kContentTypeProtobuf) == 0)) {
    if (!bulk_response) {
      auto error_response = mapper.ReadValue<ErrorResponse>(content, content_length);
      if (error_response) {
        server_error_code = error_response->error_code;
      }
    } else {
      auto error_response = mapper.ReadValue<IngestBulkErrorResponse>(content, content_length);
      if (error_response) {
        server_error_code = error_response->error_code;
      }
      bool updated = UpdateErrorResponse(error_response.get());
      if (updated) {
        payload_t converted = mapper.WriteValue(*error_response);
        delete[] content;
        content = converted.data;
        content_length = converted.length;
      }
    }
  } else {
    // The response body is not serialized ErrorResponse; generate one here
    ErrorResponse resp(std::string((const char *)content, content_length));
    payload_t converted = mapper.WriteValue(resp);
    delete[] content;
    content = converted.data;
    content_length = converted.length;
  }
  OpStatus op_status = interpreter.DetermineError(status_code, server_error_code);
  DEBUG_LOG("resolved error: %s", Statuses::Name(op_status).c_str());
  return ResponseMessage(op_status, endpoint, content, content_length);
}

bool HttpResponseHandler::UpdateErrorResponse(IngestBulkErrorResponse *error_response) {
  if (error_response->results_with_error.empty()) {
    return false;
  }
  bool translated = false;
  for (auto &result : error_response->results_with_error) {
    if (result.status_code != 0 && !interpreter.IsSuccessful(result.status_code)) {
      OpStatus newStatus = interpreter.DetermineError(result.status_code, result.server_error_code);
      result.status_code = Statuses::StatusCode(newStatus);
      translated = true;
    }
  }
  return translated;
}
};  // namespace tfos::csdk
