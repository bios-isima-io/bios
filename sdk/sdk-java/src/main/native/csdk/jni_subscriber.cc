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
 * The request subscriber that pushes requests to the JNI pipe
 */
#include "csdk/jni_subscriber.h"

#include <tfoscsdk/log.h>

#include "csdk/jni_pipe.h"
#include "csdk/jni_store.h"

using tfos::jni::JniPipe;
using tfos::jni::JniPipeFactory;

using tfos::csdk::connection_handle_t;
using tfos::csdk::ConnectionManager;
using tfos::csdk::LogicalConnection;
using tfos::csdk::LogicalPipe;
using tfos::csdk::pipe_handle_t;

JNIEXPORT jint JNICALL
Java_com_tieredfractals_tfos_sdk_csdk_reactive_impl_DefaultTfosNativeRequestSubscriber_subscribe(
    JNIEnv *env, jobject this_obj, jlong connection_id, jint pipe_handle, jobject subscription) {
  DEBUG_LOG("Pushing subscription for connection %ld", connection_id);
  int ret_val = 0;
  ConnectionManager *cm = ConnectionManager::getInstance();
  LogicalConnection *lc = cm->beginLogicalConnectionUsage(connection_id);
  if (lc == nullptr) {
    // already closed;
    ret_val = -2;
  } else {
    try {
      const LogicalPipe *lcp = lc->getPipe(pipe_handle);
      auto &pipe = dynamic_cast<JniPipe &>(lcp->getDataPipe());
      pipe.setSubscription(env, subscription);
    } catch (const std::exception &e) {
      // TODO(ramesh) : log warning..this should never happen
      ret_val = -3;
    }
  }
  cm->endLogicalConnectionUsage(connection_id);
  DEBUG_LOG("Subscription for connection %ld returned %d", connection_id, ret_val);
  return ret_val;
}

JNIEXPORT jint JNICALL
Java_com_tieredfractals_tfos_sdk_csdk_reactive_impl_DefaultTfosNativeRequestSubscriber_error(
    JNIEnv *env, jobject this_obj, jlong connection_id, jint pipe_handle) {
  // TODO(ramesh): handle errors
  return -1;
}

JNIEXPORT jint JNICALL
Java_com_tieredfractals_tfos_sdk_csdk_reactive_impl_DefaultTfosNativeRequestSubscriber_next(
    JNIEnv *env, jobject this_obj, jlong connection_id, jint pipe_handle, jobject request_wrapper,
    jobject request) {
  DEBUG_LOG("Submitting a request for connection %ld", connection_id);
  int ret_val = 0;
  ConnectionManager *cm = ConnectionManager::getInstance();
  LogicalConnection *lc = cm->beginLogicalConnectionUsage(connection_id);
  if (lc == nullptr) {
    // already closed
    ret_val = -2;
  } else {
    try {
      const LogicalPipe *lcp = lc->getPipe(pipe_handle);
      auto &pipe = dynamic_cast<JniPipe &>(lcp->getDataPipe());
      pipe.pushRequestMessage(env, request_wrapper, request);
    } catch (const std::exception &e) {
      // TODO(ramesh): log warning and return error
      ret_val = -1;
    }
  }
  cm->endLogicalConnectionUsage(connection_id);
  DEBUG_LOG("Request submission for connection %ld returned %d", connection_id, ret_val);
  return ret_val;
}

JNIEXPORT jint JNICALL
Java_com_tieredfractals_tfos_sdk_csdk_reactive_impl_DefaultTfosNativeRequestSubscriber_complete(
    JNIEnv *env, jobject thisObj, jlong connection_id, jint pipe_handle) {
  // TODO(ramesh): handle completion smoothly
  // Currently no-op; A subsequent close anyway closes the pipe
  return 0;
}
