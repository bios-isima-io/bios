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
#ifndef TFOSCSDK_UTILS_H_
#define TFOSCSDK_UTILS_H_

#include <sys/time.h>
#include <time.h>

#include <string>

#include "tfoscsdk/csdk.h"
#include "tfoscsdk/message_struct.h"
#include "tfoscsdk/models.h"
#include "tfoscsdk/status.h"

namespace tfos::csdk {

class Utils {
 public:
  static std::string GetSdkName();

  static std::string GetVersion();

  static string_t GetOperationName(CSdkOperationId id);

  static CSdkOperationId OperationNameToId(const std::string &name);

  static const char *GetActualOperationName(CSdkOperationId id);

  static OperationType GetOperationType(CSdkOperationId id);

  static bool IsProtobufOperation(CSdkOperationId id);

  static std::string GetOperationInfo(const RequestMessage *request_message);

  static bool IsRetriableFailure(OpStatus status);

  static Permission ResolvePermission(string_t src);

  static payload_t MakePayload(const char *text);

  static string_t ToStringType(const std::string &src);

  static payload_t AllocatePayload(int64_t capacity);

  static void ReleasePayload(payload_t *payload);

  static void ReleasePayload(const uint8_t *data);

  static std::string UrlToEndpoint(std::string endpoint);

  /**
   * Utility method to return an error message to the API caller.
   * Use only when endpoint is unresolvable.
   */
  static void ReplyError(OpStatus status, const std::string &error_message, capi_completion_cb cb,
                         void *cb_args);

  /**
   * Returns epoch time in milliseconds.
   */
  static inline int64_t CurrentTimeMillis() {
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return tv.tv_sec * 1000 + tv.tv_usec / 1000;
  }

  /**
   * Returns epoch time in microseconds.
   */
  static inline uint64_t CurrentTimeMicros() {
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return tv.tv_sec * 1000 * 1000 + tv.tv_usec;
  }

  /**
   * Returns clock time in milliseconds.
   * The return value is not epoch time, but the returned values are more
   * reliable for elapsed time calculation, i.e., metrics.
   */
  static inline int64_t ClockTimeMillis() {
    struct timespec tp;
    clock_gettime(CLOCK_MONOTONIC, &tp);
    return tp.tv_sec * 1000 + tp.tv_nsec / 1000000;
  }

  /**
   * Utility method to put a metric timestamp at the next position of a metrc
   * timestamps buffer.
   */
  static inline void PutTimestamp(payload_t *timestamps) {
    if (timestamps != nullptr && timestamps->data != nullptr) {
      int64_t *timestamp = reinterpret_cast<int64_t *>(timestamps->data + timestamps->length);
      *timestamp = ClockTimeMillis();
      timestamps->length += sizeof(*timestamp);
    }
  }

  static std::string UrlEncode(const std::string &src);

  static completion_data_t get_basic_completion_data();

 private:
  static void AppendHexChar(char ch, std::string *str);
};

}  // namespace tfos::csdk
#endif  // TFOSCSDK_UTILS_H_
