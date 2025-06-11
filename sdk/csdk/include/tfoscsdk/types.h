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
#ifndef TFOSCSDK_TYPES_H_
#define TFOSCSDK_TYPES_H_

/*
 * Definitions of types that are used by API methods of C-SDK.
 * Note that these types do not have namespaces beaucse the
 * interface should work for C langage, too.
 */

#include <stddef.h>

#ifdef __cplusplus
#include <cstdint>
extern "C" {
#else
#include <stdint.h>
#endif

typedef int32_t status_code_t;

typedef struct payload {
  uint8_t *data;
  int64_t length;
} payload_t;

typedef struct mystring {
  const char *text;
  int32_t length;
} string_t;

// generic type to hold a C-API object
// typedef void *capi_t;
typedef int32_t capi_t;

/**
 * Struct to carry information from CSDK to language SDK when a completion callback is invoked.
 */
typedef struct completion_data {
  // Endpoint (server) that sent the response.
  string_t endpoint;

  // Whether a QoS retry was considered - e.g. only for realtime applications, certain operations.
  bool qos_retry_considered;
  // Whether a QoS retry was sent - e.g. if the first request's response did not arrive in time.
  bool qos_retry_sent;
  // Whether the response of a QoS retry was used - true only if the response of the retry
  // (second request) came back first, so that it was used to satisfy the application's request,
  // thereby making the QoS retry feature useful.
  bool qos_retry_response_used;
} completion_data_t;

/**
 * Callback function executed when an API operation is completed.
 *
 * @params status_code Operation result status code
 * @params response_data Serialized response object
 * @params cb_args Arguments given by the caller
 */
typedef void (*capi_completion_cb)(status_code_t status_code, completion_data_t *completion_data,
                                   payload_t response_data, void *cb_args);

#ifdef __cplusplus
}  // extern "C"
#endif
#endif  // TFOSCSDK_TYPES_H_
