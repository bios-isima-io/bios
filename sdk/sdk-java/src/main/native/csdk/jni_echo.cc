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
 * Echo controller for end to end echo testing
 */
#include "csdk/jni_echo.h"

#include <tfoscsdk/log.h>

#include "tfoscsdk/api/echo_control.h"

using tfos::csdk::EchoControl;

JNIEXPORT void JNICALL
Java_com_tieredfractals_tfos_sdk_csdk_reactive_impl_DefaultNativeEchoController_start(
    JNIEnv *env, jobject this_obj) {
  DEBUG_LOG("Starting Echo Service ");
  EchoControl::getInstance()->Start();
}

JNIEXPORT void JNICALL
Java_com_tieredfractals_tfos_sdk_csdk_reactive_impl_DefaultNativeEchoController_stop(
    JNIEnv *env, jobject this_obj) {
  DEBUG_LOG("Stopping Echo Service ");
  EchoControl::getInstance()->Stop();
}

JNIEXPORT jint JNICALL
Java_com_tieredfractals_tfos_sdk_csdk_reactive_impl_DefaultNativeEchoController_startEchoPipe(
    JNIEnv *env, jobject this_obj) {
  DEBUG_LOG("Starting Echo Pipe ");
  return EchoControl::getInstance()->StartEchoPipe();
}

JNIEXPORT void JNICALL
Java_com_tieredfractals_tfos_sdk_csdk_reactive_impl_DefaultNativeEchoController_stopEchoPipe(
    JNIEnv *env, jobject this_obj, jint pipe_id) {
  DEBUG_LOG("Stopping Echo Pipe %d", pipe_id);
  EchoControl::getInstance()->KillEchoPipe(pipe_id);
}
