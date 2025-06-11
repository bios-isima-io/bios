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
 * Java language bi-directional data pipe implementation using JNI
 */
#ifndef JNI_PIPE_H_
#define JNI_PIPE_H_

#include <jni.h>

#include <atomic>

#include "csdk/jni_store.h"
#include "tfoscsdk/api/logical_pipe.h"

namespace tfos::jni {

/**
 * Encapsulates JNI specific operations on opened logical message pipe.
 *
 * Lifecycle is controlled by the CSDK through the DataPipeFactory
 */
class JniPipe : public csdk::DataPipe {
 public:
  JniPipe() = delete;
  JniPipe(JniPipe &&other) = delete;
  JniPipe &operator=(JniPipe &&other) = delete;

  // JNI side calls to logical pipe
  bool open(JNIEnv_ *env, jobject req_publisher, jobject req_subscriber, jobject resp_pusher);
  void setSubscription(JNIEnv_ *env, jobject subscription);
  void pushRequestMessage(JNIEnv_ *env, jobject request_wrapper, jobject request);

  // native thread calls that may callback jvm
  csdk::env_handle_t attach() override;
  void request(csdk::env_handle_t env_handle, int n) const override;
  bool pushResponse(csdk::env_handle_t env_handle,
                    const csdk::response_message_t &response) const override;
  void close(csdk::env_handle_t env_handle, bool success) override;
  void detach(csdk::env_handle_t env_handle) override;

  csdk::pipe_handle_t getPipeHandle() const override;

  friend class JniPipeFactory;

 protected:
 private:
  static const int MAX_ENV_HANDLES = 5;

  JniPipe(csdk::connection_handle_t, csdk::pipe_handle_t pipe_handle, std::any store_ref,
          csdk::LogicalPipe *p_logical_pipe);
  ~JniPipe() override;

  const ConnectionStore &store;
  const csdk::pipe_handle_t pipe_handle_;
  csdk::LogicalPipe *p_logical_pipe_;

  jobject request_publisher_;
  jobject request_subscriber_;
  jobject response_pusher_;
  bool opened_;
  csdk::connection_handle_t connection_id_;
  std::atomic_int current_init_count_;
  JNIEnv_ *env_info_[MAX_ENV_HANDLES];

  // cached VM and method IDs
  JavaVM *cached_vm_;
  jmethodID rqp_subscribe_mid_;
  jmethodID rqs_onsubscribe_mid_;
  jmethodID rqs_onnext_mid_;
  jmethodID rqs_oncomplete_mid_;
  jmethodID rqsn_subscription_mid_;
  jmethodID rsp_nextresp_mid_;
  jmethodID rsp_completed_mid_;
  jmethodID rqw_get_op_id_mid_;
  jmethodID rqw_get_req_size_mid_;

  volatile jobject subscription_;

  void cacheMethodIds(JNIEnv_ *env);
};

class JniPipeFactory : public csdk::DataPipeFactory {
 public:
  JniPipeFactory(JniPipeFactory &&other) = delete;
  JniPipeFactory &operator=(JniPipeFactory &&other) = delete;

  csdk::DataPipe *create(csdk::connection_handle_t connection_id, csdk::pipe_handle_t pipe_handle,
                         std::any storeRef, csdk::LogicalPipe *p_logical_pipe) const override;

  static const JniPipeFactory &getInstance();

 private:
  JniPipeFactory() = default;
  ~JniPipeFactory() override = default;
};

}  // namespace tfos::jni

#endif /* JNI_PIPE_H_ */
