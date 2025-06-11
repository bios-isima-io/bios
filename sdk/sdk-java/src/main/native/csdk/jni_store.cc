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
 * JNI store for per connection record keeping
 */
#include "csdk/jni_store.h"

#include <tfoscsdk/log.h>

#include <cassert>

namespace tfos::jni {

const char *ENCODER_DESC =
    "(Lcom/tieredfractals/tfos/models/TfosMessage;Ljava/nio/ByteBuffer;"
    "Lcom/tieredfractals/tfos/sdk/csdk/TfosBufferFactory;)I";
const char *ENCODE_NAME = "encode";
const char *DECODER_DESC =
    "(Ljava/nio/ByteBuffer;Ljava/lang/Class;)"
    "Lcom/tieredfractals/tfos/models/TfosMessage;";
const char *DECODE_NAME = "decode";

ConnectionStore::ConnectionStore() noexcept
    : msg_codec_(nullptr),
      connection_id_(0),
      encoder_mid_(nullptr),
      decoder_mid_(nullptr),
      opened_(false) {}

bool ConnectionStore::open(JNIEnv_ *env, jobject codec, csdk::connection_handle_t connection_id) {
  msg_codec_ = env->NewGlobalRef(codec);
  if (nullptr == msg_codec_) {
    return false;
  }
  connection_id_ = connection_id;

  // cache methodID for request encoder
  jclass clazz = env->GetObjectClass(msg_codec_);
  encoder_mid_ = env->GetMethodID(clazz, ENCODE_NAME, ENCODER_DESC);
  assert(nullptr != encoder_mid_);

  decoder_mid_ = env->GetMethodID(clazz, DECODE_NAME, DECODER_DESC);
  assert(nullptr != decoder_mid_);

  opened_.store(true);
  return true;
}

int64_t ConnectionStore::encode(JNIEnv_ *env, jobject request_message, uint8_t *req_buffer,
                                int64_t capacity) const {
  assert(opened_.load());

  DEBUG_LOG("Encoding with max capacity %ld", capacity);
  jobject direct_buffer = env->NewDirectByteBuffer(req_buffer, capacity);
  if (direct_buffer == nullptr) {
    return false;
  }
  auto id = env->CallIntMethod(msg_codec_, encoder_mid_, request_message, direct_buffer, nullptr);
  if (env->ExceptionCheck() == JNI_TRUE) {
    // TODO(ramesh) : allow dynamic size buffers when message is bigger than
    // given capacity
    DEBUG_LOG("Not implemented yet");
  }
  DEBUG_LOG("Encoded with length %d", id);
  return id;
}

void ConnectionStore::close(JNIEnv_ *env) {
  env->DeleteGlobalRef(msg_codec_);
  opened_.store(false);
}

}  // namespace tfos::jni
