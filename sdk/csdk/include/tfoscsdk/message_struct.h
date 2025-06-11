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

/**
 * Message structures used for transporting messages from the logical to the
 * physical realm within CSDK.
 */
#ifndef TFOSCSDK_COMMON_MESSAGE_STRUCT_H_
#define TFOSCSDK_COMMON_MESSAGE_STRUCT_H_

#include <cstdint>
#include <string>

#include "tfoscsdk/csdk.h"
#include "tfoscsdk/models.h"
#include "tfoscsdk/properties.h"
#include "tfoscsdk/status.h"

namespace tfos::csdk {

typedef int32_t queue_handle_t;
typedef int32_t entity_handle_t;

typedef struct {
  int32_t op_id_;
  const payload_t payload_;
  void *context_;
} request_message_t;

/**
 * (mostly) Immutable struct to carry an operation response.
 *
 * TODO(Naoki): We may want to use memory pool for managing response_buffer_
 * memory. But using jemalloc may be good enough.
 */
struct response_message_t {
  const uint32_t status_;
  uint8_t *const response_buffer_;
  const int64_t message_length_;

  /**
   * Constructor of this struct.
   * Since the struct is immutable, initialization should be either by this
   * constructor or by copying the instance.
   *
   * @param status
   *     Result status of the operation.
   * @param response_buffer
   *     Response message body. Memory allocation must be done by new uint8_t[].
   *     Malloc must not be used.
   */
  response_message_t(uint32_t status, uint8_t *response_buffer, int64_t message_length)
      : status_(status), response_buffer_(response_buffer), message_length_(message_length) {}

  /**
   * Method to release resources that this message holds.
   * This method should be called after the receiver consumes this message.
   */
  void release_resources() { delete[] response_buffer_; }
};

class RequestMessage {
 public:
  DISABLE_COPY_MOVE_AND_ASSIGN(RequestMessage);

  RequestMessage() : op_id_(CSDK_OP_NOOP), payload_({0}), context_(nullptr) {}

  RequestMessage(const request_message_t &msg_from_pipe)
      : op_id_(static_cast<CSdkOperationId>(msg_from_pipe.op_id_)),
        payload_(msg_from_pipe.payload_),
        context_(reinterpret_cast<RequestContext *>(msg_from_pipe.context_)) {}

  ~RequestMessage() = default;

  CSdkOperationId op_id() const { return op_id_; }

  const uint8_t *req_message() const { return payload_.data; }

  const int64_t req_message_length() const { return payload_.length; }

  const payload_t payload() const { return payload_; }

  RequestContext *context() const { return context_; }

 private:
  const CSdkOperationId op_id_;
  const payload_t payload_;
  RequestContext *const context_;
};

class ResponseMessage {
 private:
  OpStatus status_;
  std::string endpoint_;
  uint8_t *response_body_;
  int64_t body_length_;
  bool qos_retry_considered_ = false;
  bool qos_retry_sent_ = false;
  bool qos_retry_response_used_ = false;

 public:
  /**
   * Constructor of this class.
   *
   * @param status
   *     Result status of the operation.
   * @param endpoint
   *     Remote endpoint name.
   * @param response_body
   *     Response message body. Memory allocation must be done by new uint8_t[].
   *     Malloc must not be used.
   * @param body_length
   *     Response message body length.
   */
  ResponseMessage(OpStatus status, const std::string &endpoint, uint8_t *response_body,
                  int64_t body_length)
      : status_(status),
        endpoint_(endpoint),
        response_body_(response_body),
        body_length_(body_length) {}

  /**
   * Constructor of this class.
   *
   * This constructor is used when we don't send back endpoint name to the
   * caller, i.e., the operation is successful and the build mode is not debug.
   *
   * @param status
   *     Result status of the operation.
   * @param endpoint
   *     Remote endpoint name.
   * @param response_body
   *     Response message body. Memory allocation must be done by new uint8_t[].
   *     Malloc must not be used.
   * @param body_length
   *     Response message body length.
   */
  ResponseMessage(OpStatus status, uint8_t *response_body, int64_t body_length)
      : status_(status), response_body_(response_body), body_length_(body_length) {}

  ResponseMessage(OpStatus status, const std::string &endpoint, const payload_t &reply)
      : status_(status),
        endpoint_(endpoint),
        response_body_(reply.data),
        body_length_(reply.length) {}

  ResponseMessage() : status_(OpStatus::OK), response_body_(nullptr), body_length_(0) {}

  ~ResponseMessage() = default;

  inline void set_qos_retry_considered(bool value) { qos_retry_considered_ = value; }

  inline void set_qos_retry_sent(bool value) { qos_retry_sent_ = value; }

  inline void set_qos_retry_response_used(bool value) { qos_retry_response_used_ = value; }

  inline bool qos_retry_considered() const { return qos_retry_considered_; }

  inline bool qos_retry_sent() const { return qos_retry_sent_; }

  inline bool qos_retry_response_used() const { return qos_retry_response_used_; }

  inline OpStatus status() const { return status_; }

  inline const std::string &endpoint() const { return endpoint_; }

  inline const uint8_t *response_body() const { return response_body_; }

  int64_t body_length() const { return body_length_; };

  inline payload_t payload() const { return {.data = response_body_, .length = body_length_}; };

  response_message_t response_message() const {
    return response_message_t(Statuses::StatusCode(status_), response_body_, body_length_);
  }
};

// TODO(Naoki): I don't know what this function does but will preserve the
// original signature as much as possible for the least impact. C function in
// the C++ implementation. Move this function into some class.
static inline bool isRequestPath() { return false; }

}  //  namespace tfos::csdk

#endif  // TFOSCSDK_COMMON_MESSAGE_STRUCT_H_
