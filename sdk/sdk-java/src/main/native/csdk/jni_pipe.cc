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
 * JNI <---> CSDK bi-directional message data pipe implementation
 */
#include "csdk/jni_pipe.h"

#include <tfoscsdk/log.h>

#include <cassert>

#include "tfoscsdk/properties.h"

namespace tfos::jni {

using csdk::Properties;
using csdk::request_message_t;
using csdk::response_message_t;

const char *RQP_SUBSCRIBE_NAME = "subscribe";
const char *RQP_SUBSCRIBE_DESC = "(Lcom/tieredfractals/tfos/sdk/csdk/reactive/TfosSubscriber;)V";

const char *RQS_ONSUBSCRIBE_NAME = "onSubscribe";
const char *RQS_ONSUBSCRIBE_DESC =
    "(Lcom/tieredfractals/tfos/sdk/csdk/reactive/TfosSubscription;)V";

const char *RQS_ONNEXT_NAME = "onNext";
const char *RQS_ONNEXT_DESC = "(Ljava/lang/Object;)V";

const char *RQS_ONCOMPLETE_NAME = "onComplete";
const char *RQS_ONCOMPLETE_DESC = "()V";

const char *RQSN_SUBSCRIPTION_NAME = "request";
const char *RQSN_SUBSCRIPTION_DESC = "(J)V";

const char *RSP_NEXT_RESP_NAME = "nextResponse";
const char *RSP_NEXT_RESP_DESC = "(Ljava/nio/ByteBuffer;Z)Z";

const char *RSP_COMPLETED_NAME = "completed";
const char *RSP_COMPLETED_DESC = "(Z)V";

const char *RQW_GET_OP_ID_NAME = "getOpId";
const char *RQW_GET_OP_ID_DESC = "()I";

const char *RQW_GET_REQ_SIZE_NAME = "getRequestSize";
const char *RQW_GET_REQ_SIZE_DESC = "()I";

JniPipe::JniPipe(csdk::connection_handle_t connection_id, csdk::pipe_handle_t pipe_handle,
                 std::any store_ref, csdk::LogicalPipe *p_logical_pipe)
    : store(*std::any_cast<ConnectionStore *>(store_ref)),
      pipe_handle_(pipe_handle),
      p_logical_pipe_(p_logical_pipe),
      request_publisher_(nullptr),
      request_subscriber_(nullptr),
      response_pusher_(nullptr),
      opened_(false),
      connection_id_(connection_id),
      current_init_count_(0),
      env_info_{nullptr},
      cached_vm_(nullptr),
      rqp_subscribe_mid_(nullptr),
      rqs_onsubscribe_mid_(nullptr),
      rqs_onnext_mid_(nullptr),
      rqs_oncomplete_mid_(nullptr),
      rqsn_subscription_mid_(nullptr),
      rsp_nextresp_mid_(nullptr),
      rsp_completed_mid_(nullptr),
      rqw_get_op_id_mid_(nullptr),
      rqw_get_req_size_mid_(nullptr),
      subscription_(nullptr) {}

JniPipe::~JniPipe() = default;

bool JniPipe::open(JNIEnv_ *env, jobject req_publisher, jobject req_subscriber,
                   jobject resp_pusher) {
  if (env->GetJavaVM(&cached_vm_) != JNI_OK) {
    return false;
  }

  request_publisher_ = env->NewGlobalRef(req_publisher);
  if (nullptr == request_publisher_) {
    return false;
  }

  request_subscriber_ = env->NewGlobalRef(req_subscriber);
  if (nullptr == request_subscriber_) {
    env->DeleteGlobalRef(request_publisher_);
    request_publisher_ = nullptr;
    return false;
  }

  response_pusher_ = env->NewGlobalRef(resp_pusher);
  if (nullptr == response_pusher_) {
    env->DeleteGlobalRef(request_publisher_);
    env->DeleteGlobalRef(request_subscriber_);
    request_publisher_ = request_subscriber_ = nullptr;
    return false;
  }

  cacheMethodIds(env);
  opened_ = true;
  return true;
}

void JniPipe::setSubscription(JNIEnv_ *env, jobject s) {
  // cache methodID for request subscription
  // NOTE: cache before subscription is set non null..so that it reflects on all
  // thread as susbcription pointer is a volatile write
  DEBUG_LOG("Setting Subscription");
  jclass clazz = env->GetObjectClass(s);
  rqsn_subscription_mid_ = env->GetMethodID(clazz, RQSN_SUBSCRIPTION_NAME, RQSN_SUBSCRIPTION_DESC);
  assert(nullptr != rqsn_subscription_mid_);

  subscription_ = env->NewGlobalRef(s);
  // if subscription is still NULL, rest of methods will close the gate

  // Now that subscription has arrived, inform CSDK that the pipe is ready for
  // action
  p_logical_pipe_->signalPipeReady();
  DEBUG_LOG("Pipe is ready");
}

void JniPipe::pushRequestMessage(JNIEnv_ *env, jobject request_wrapper, jobject request) {
  uint8_t *request_buffer_ = p_logical_pipe_->allocateBuffer(Properties::DEFAULT_BUFFER_SIZE);

  // due to pre-allocation and flow api, it will be unlikely that request buffer
  // is not obtained so assert for now
  assert(request_buffer_ != nullptr);

  int64_t message_length_ =
      store.encode(env, request, request_buffer_, Properties::DEFAULT_BUFFER_SIZE);
  assert(message_length_ > 0);

  if (rqw_get_req_size_mid_ == nullptr) {
    // cache method ID once
    jclass clazz = env->GetObjectClass(request_wrapper);
    rqw_get_op_id_mid_ = env->GetMethodID(clazz, RQW_GET_OP_ID_NAME, RQW_GET_OP_ID_DESC);
    rqw_get_req_size_mid_ = env->GetMethodID(clazz, RQW_GET_REQ_SIZE_NAME, RQW_GET_REQ_SIZE_DESC);
  }

  auto op_id = env->CallIntMethod(request_wrapper, rqw_get_op_id_mid_);
  auto operation_id = static_cast<CSdkOperationId>(op_id);

  DEBUG_LOG("Operation Id is %d, Length is %ld", operation_id, message_length_);

  tfos::csdk::request_message_t msg = {
      .op_id_ = operation_id, .payload_ = {.data = request_buffer_, .length = message_length_}};

  // csdk is expected to copy all header information, while payload does not
  // need a copy as the buffer for message is provided by csdk itself.
  p_logical_pipe_->pushRequest(msg);
}

csdk::env_handle_t JniPipe::attach() {
  csdk::env_handle_t env_handle = current_init_count_++;
  assert(env_handle < MAX_ENV_HANDLES);
  assert(opened_);

  if (cached_vm_->AttachCurrentThread(reinterpret_cast<void **>(&env_info_[env_handle]), nullptr) !=
      JNI_OK) {
    return -1;
  }

  return env_handle;
}

void JniPipe::request(csdk::env_handle_t env_handle, int capacity) const {
  assert(env_handle >= 0 && env_handle < MAX_ENV_HANDLES);
  assert(this->subscription_ != nullptr);
  JNIEnv_ *env = env_info_[env_handle];

  env->CallVoidMethod(subscription_, rqsn_subscription_mid_, capacity);
  // TODO(ramesh): Handle exception
}

bool JniPipe::pushResponse(csdk::env_handle_t env_handle,
                           const response_message_t &response) const {
  assert(env_handle >= 0 && env_handle < MAX_ENV_HANDLES);
  assert(this->subscription_ != nullptr);

  JNIEnv_ *env = env_info_[env_handle];

  // TODO(ramesh): Handle possible error status in the response.
  jobject resp_buf = env->NewDirectByteBuffer(response.response_buffer_, response.message_length_);

  jboolean result = env->CallBooleanMethod(response_pusher_, rsp_nextresp_mid_, resp_buf, false);
  // TODO(ramesh): Handle exception
  return result;
}

void JniPipe::close(csdk::env_handle_t env_handle, bool success) {
  assert(env_handle >= 0 && env_handle < MAX_ENV_HANDLES);
  JNIEnv_ *env = env_info_[env_handle];
  assert(env != nullptr);

  if (opened_) {
    env->CallVoidMethod(response_pusher_, rsp_completed_mid_, success);

    env->DeleteGlobalRef(request_subscriber_);
    env->DeleteGlobalRef(request_publisher_);
    env->DeleteGlobalRef(response_pusher_);
    if (subscription_ != nullptr) {
      env->DeleteGlobalRef(subscription_);
    }
  }
  opened_ = false;
}

void JniPipe::detach(csdk::env_handle_t env_handle) {
  assert(env_handle >= 0 && env_handle < MAX_ENV_HANDLES);
  JNIEnv_ *env = env_info_[env_handle];

  if (env != nullptr) {
    cached_vm_->DetachCurrentThread();
    env_info_[env_handle] = nullptr;
  }
}

csdk::pipe_handle_t JniPipe::getPipeHandle() const { return pipe_handle_; }

void JniPipe::cacheMethodIds(JNIEnv_ *env) {
  // cache methodID for request publisher
  jclass clazz = env->GetObjectClass(request_publisher_);
  rqp_subscribe_mid_ = env->GetMethodID(clazz, RQP_SUBSCRIBE_NAME, RQP_SUBSCRIBE_DESC);
  assert(nullptr != rqp_subscribe_mid_);

  // cache methodIDs for request subscriber
  clazz = env->GetObjectClass(request_subscriber_);
  rqs_onsubscribe_mid_ = env->GetMethodID(clazz, RQS_ONSUBSCRIBE_NAME, RQS_ONSUBSCRIBE_DESC);
  rqs_onnext_mid_ = env->GetMethodID(clazz, RQS_ONNEXT_NAME, RQS_ONNEXT_DESC);
  rqs_oncomplete_mid_ = env->GetMethodID(clazz, RQS_ONCOMPLETE_NAME, RQS_ONCOMPLETE_DESC);
  assert(nullptr != rqs_onsubscribe_mid_ && nullptr != rqs_onnext_mid_ &&
         nullptr != rqs_oncomplete_mid_);

  // cache methodIDs for response pusher
  clazz = env->GetObjectClass(response_pusher_);
  rsp_nextresp_mid_ = env->GetMethodID(clazz, RSP_NEXT_RESP_NAME, RSP_NEXT_RESP_DESC);
  rsp_completed_mid_ = env->GetMethodID(clazz, RSP_COMPLETED_NAME, RSP_COMPLETED_DESC);
  assert(nullptr != rsp_nextresp_mid_ && nullptr != rsp_completed_mid_);
}

////////// JNI PIPE FACTORY /////////

csdk::DataPipe *JniPipeFactory::create(csdk::connection_handle_t connection_id,
                                       csdk::pipe_handle_t pipe_handle, std::any store_ref,
                                       csdk::LogicalPipe *p_logical_pipe) const {
  return new JniPipe(connection_id, pipe_handle, store_ref, p_logical_pipe);
}

const JniPipeFactory &JniPipeFactory::getInstance() {
  static JniPipeFactory pipeFactory;
  return pipeFactory;
}

}  // namespace tfos::jni
