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
#ifndef TFOSCSDK_HTTP_RESPONSE_HANDLER_H_
#define TFOSCSDK_HTTP_RESPONSE_HANDLER_H_

#include "tfoscsdk/message_struct.h"

namespace tfos::csdk {

class Logger;
class HttpResponseHandler {
 public:
  static ResponseMessage HandleResponse(uint32_t status_code, const std::string &content_type,
                                        uint8_t *content, uint64_t content_length,
                                        const std::string &endpoint, bool bulk_response);

 private:
  static bool UpdateErrorResponse(IngestBulkErrorResponse *error_response);
};
}  // namespace tfos::csdk
#endif  // TFOSCSDK_HTTP_RESPONSE_HANDLER_H_
