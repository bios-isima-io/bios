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
 * Main JNI interface to CSDK
 */
#include "csdk/jni_connector.h"

#include <cassert>

#include "csdk/jni_pipe.h"
#include "csdk/jni_store.h"
#include "tfoscsdk/api/connection_manager.h"
#include "tfoscsdk/api/dapi_bridge.h"
#include "tfoscsdk/api/data_pipe.h"
#include "tfoscsdk/api/logical_connection.h"

using tfos::jni::ConnectionStore;
using tfos::jni::JniPipe;
using tfos::jni::JniPipeFactory;

using tfos::csdk::connection_handle_t;
using tfos::csdk::ConnectionManager;
using tfos::csdk::DapiBridge;
using tfos::csdk::LogicalConnection;
using tfos::csdk::LogicalPipe;
using tfos::csdk::pipe_handle_t;

JNIEXPORT void JNICALL
Java_com_tieredfractals_tfos_sdk_csdk_reactive_impl_DefaultTfosNativeBridge_init(JNIEnv *env,
                                                                                 jobject this_obj,
                                                                                 jint capi) {
  DapiBridge::getInstance()->initialize(capi);
}

/**
 * Open logical connection.
 *
 * @param env            JNI env for this thread
 * @param thisObj        Java object invoking this native call
 * @param sessionId      session ID
 * @param codec          Reference to the request encoder
 *
 * @return connection ID which is a increasing sequence number that keeps
 * rolling on overflow
 */
JNIEXPORT jlong JNICALL
Java_com_tieredfractals_tfos_sdk_csdk_reactive_impl_DefaultTfosNativeBridge_open(JNIEnv *env,
                                                                                 jobject this_obj,
                                                                                 jint session_id,
                                                                                 jobject codec) {
  // checkout a logical session object from csdk
  ConnectionManager *cm = ConnectionManager::getInstance();
  connection_handle_t connection_id =
      cm->checkoutConnection(JniPipeFactory::getInstance(), session_id);
  if (connection_id == ConnectionManager::INVALID_CONNECTION_HANDLE) {
    // TODO(ramesh): define error codes in JNI
    return 0;
  }
  LogicalConnection *lc = cm->beginLogicalConnectionUsage(connection_id);
  try {
    assert(lc != nullptr);

    // create and open jni connection store for jni record keeping
    auto *csp = new ConnectionStore();
    csp->open(env, codec, connection_id);

    // Store the connection store pointer for future retrieval
    const std::any &store = std::make_any<ConnectionStore *>(csp);
    lc->storeConnectionData(store);
    cm->endLogicalConnectionUsage(connection_id);
  } catch (const std::exception &e) {
    cm->endLogicalConnectionUsage(connection_id);
    cm->releaseConnection(connection_id);
    connection_id = ConnectionManager::INVALID_CONNECTION_HANDLE;
  }
  return connection_id;
}

/**
 * Open  a message Pipe under the given logical connection.
 *
 * @param env            JNI env for this thread
 * @param thisObj        Java object invoking this native call
 * @param connection_id   ID of the logical connection
 * @param req_publisher   Reference to the request message publisher object
 * @param req_subscriber  Reference to the request message subscriber object
 * @param resp_pusher     Reference to the response object to which responses
 * can be pushed
 *
 * @return pipe ID
 */
JNIEXPORT jint JNICALL
Java_com_tieredfractals_tfos_sdk_csdk_reactive_impl_DefaultTfosNativeBridge_openPipe(
    JNIEnv *env, jobject this_obj, jlong connection_id, jobject req_publisher,
    jobject req_subscriber, jobject resp_pusher) {
  int ret_value = -1;
  ConnectionManager *cm = ConnectionManager::getInstance();
  LogicalConnection *lc = cm->beginLogicalConnectionUsage(connection_id);
  if (lc == nullptr) {
    // already closed
    ret_value = -2;
  } else {
    try {
      pipe_handle_t pipe_handle = lc->openPipe();
      if (pipe_handle < 0) {
        ret_value = -3;
      } else {
        auto &jni_pipe = dynamic_cast<JniPipe &>(lc->getPipe(pipe_handle)->getDataPipe());
        bool success = jni_pipe.open(env, req_publisher, req_subscriber, resp_pusher);
        if (success) {
          ret_value = jni_pipe.getPipeHandle();
        }
      }
    } catch (const std::bad_cast &e) {
      ret_value = -3;
    } catch (const std::exception &e) {
      ret_value = -3;
    }
  }
  cm->endLogicalConnectionUsage(connection_id);
  return ret_value;
}

/**
 * Close message pipe.
 *
 * @param env             JNI env for this thread
 * @param thisObj         Java object invoking this native call
 * @param connection_id   ID of the logical connection
 * @param pipe_handle     ID of the pipe to be closed
 * @param force_flag      If true, pipe will be closed without waiting for
 * messages to be flushed in the pipe
 */
JNIEXPORT void JNICALL
Java_com_tieredfractals_tfos_sdk_csdk_reactive_impl_DefaultTfosNativeBridge_closePipe(
    JNIEnv *env, jobject this_obj, jlong connection_id, jint pipe_handle, jboolean force_flag) {
  ConnectionManager *cm = ConnectionManager::getInstance();
  LogicalConnection *lc = cm->beginLogicalConnectionUsage(connection_id);
  if (lc == nullptr) {
    // already closed
    return;
  }
  try {
    assert(pipe_handle > 0);
    // Since a closed pipe can never be reused in the same session, close is
    // idempotent
    lc->getPipe(pipe_handle)->asyncClosePipe(force_flag);
  } catch (std::exception &e) {
    // TODO(ramesh): log warning and handle this error
  }
  cm->endLogicalConnectionUsage(connection_id);
}

/**
 * Close the logical connection.
 *
 * If there are open pipes, it will close them forcefully and waits for all
 * pipes to be closed and the connection released. All this is assumed to be
 * done inside {@code releaseConnection}
 *
 * @param env            JNI env for this thread
 * @param thisObj        Java object invoking this native call
 * @param connection_id   Logical connection ID
 */
JNIEXPORT void JNICALL
Java_com_tieredfractals_tfos_sdk_csdk_reactive_impl_DefaultTfosNativeBridge_close(
    JNIEnv *env, jobject thisObj, jlong connection_id) {
  ConnectionManager *cm = ConnectionManager::getInstance();
  LogicalConnection *lc = cm->beginLogicalConnectionUsage(connection_id);
  if (lc == nullptr) {
    // already closed
    return;
  }
  const std::any &any_store = lc->getConnectionData();
  try {
    auto *store = std::any_cast<ConnectionStore *>(any_store);
    cm->endLogicalConnectionUsage(connection_id);
    // release connection is synchronous and will block until all open pipes are
    // closed forcefully
    cm->releaseConnection(connection_id);
    store->close(env);
    delete (store);
  } catch (const std::bad_any_cast &e) {
    // TODO(ramesh): log warning..this should not happen
    cm->endLogicalConnectionUsage(connection_id);
  }
}
