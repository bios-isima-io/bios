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
 * JNI Store for each logical connection
 */
#ifndef JNI_STORE_H_
#define JNI_STORE_H_

#include <jni.h>

#include <atomic>

#include "tfoscsdk/api/connection_manager.h"

namespace tfos::jni {

class ConnectionStore {
 private:
  jobject msg_codec_;
  csdk::connection_handle_t connection_id_;
  jmethodID encoder_mid_;
  jmethodID decoder_mid_;
  std::atomic_bool opened_;

 public:
  ConnectionStore(ConnectionStore &other) = delete;
  ConnectionStore &operator=(ConnectionStore const &other) = delete;
  ConnectionStore() noexcept;
  ~ConnectionStore() = default;

  bool open(JNIEnv_ *env, jobject codec, csdk::connection_handle_t connection_id);
  int64_t encode(JNIEnv_ *env, jobject request_message, uint8_t *req_buffer,
                 int64_t capacity) const;
  void close(JNIEnv_ *env);
};

}  // namespace tfos::jni

#endif /* JNI_STORE_H_ */
