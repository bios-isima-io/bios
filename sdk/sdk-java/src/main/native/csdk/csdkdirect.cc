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
#include "csdk/csdkdirect.h"

#include <sys/time.h>
#include <time.h>

#include <cassert>
#include <iostream>

#include "tfoscsdk/directapi.h"

static const int kMaxTempArraySize = 32;

class JniConfig {
 private:
  JavaVM *jvm_;
  jint jni_version_;
  capi_t capi_;

  jclass callback_center_;
  jmethodID on_completion_;
  jmethodID more_ingest_bulk_;

 public:
  JniConfig(JNIEnv *env, capi_t capi, jclass callback_center, jmethodID on_completion,
            jmethodID more_ingest_bulk)
      : capi_(capi),
        callback_center_(callback_center),
        on_completion_(on_completion),
        more_ingest_bulk_(more_ingest_bulk) {
    env->GetJavaVM(&jvm_);
    jni_version_ = env->GetVersion();
  }

  // Getters
  inline JavaVM *jvm() const { return jvm_; }
  inline jint jni_version() const { return jni_version_; }
  inline capi_t capi() const { return capi_; }
  inline jclass callback_center() const { return callback_center_; }
  inline jmethodID on_completion() const { return on_completion_; }
  inline jmethodID more_ingest_bulk() const { return more_ingest_bulk_; }
};

static JniConfig *jni_config = nullptr;

static string_t MakeString(JNIEnv *env, jstring src);
static void ReleaseString(JNIEnv *env, jstring src, const string_t &to_release);

static inline int64_t GetTimestamp() {
  struct timespec tp;
  clock_gettime(CLOCK_MONOTONIC, &tp);
  int64_t timestamp = tp.tv_sec * 1000 + tp.tv_nsec / 1000000;
  return timestamp;
}

static inline int64_t CurrentTimeMicros() {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return tv.tv_sec * 1000 * 1000 + tv.tv_usec;
}

/*
 * Class:     io_isima_bios_sdk_csdk_CSdkDirect
 * Method:    initialize
 * Signature: (Ljava/lang/Class;Ljava/lang/String;)I
 */
jint JNICALL Java_io_isima_bios_sdk_csdk_CSdkDirect_initialize(JNIEnv *env, jobject obj,
                                                               jclass callback_center,
                                                               jstring modules_profile) {
  if (jni_config == nullptr) {
    const char *type = env->GetStringUTFChars(modules_profile, nullptr);
    capi_t capi = DaInitialize(type);
    env->ReleaseStringUTFChars(modules_profile, type);
    if (capi < 0) {
      return -1;
    }

    jmethodID on_completion =
        env->GetStaticMethodID(callback_center, "onCompletion",
                               "(Lio/isima/bios/sdk/csdk/CSdkApiCaller;"
                               "ILjava/lang/String;Ljava/nio/ByteBuffer;JJJJJJ)V");
    DEBUG_LOG("method onCompletion=%p", on_completion);
    jmethodID more_ingest_bulk = env->GetStaticMethodID(
        callback_center, "moreIngestBulk", "(Lio/isima/bios/sdk/csdk/InsertBulkExecutor;JII)V");
    DEBUG_LOG("method moreIngestBulk=%p", more_ingest_bulk);
    jni_config =
        new JniConfig(env, capi, reinterpret_cast<jclass>(env->NewGlobalRef(callback_center)),
                      on_completion, more_ingest_bulk);
  }
  return jni_config->capi();
}

/*
 * Class:     io_isima_bios_sdk_csdk_CSdkDirect
 * Method:    isInitialized
 * Signature: ()Z
 */
jboolean JNICALL Java_io_isima_bios_sdk_csdk_CSdkDirect_isInitialized(JNIEnv *, jobject) {
  return jni_config != nullptr;
}

struct CallContext {
  jobject callback_instance;
  jobject data;
  payload_t timestamps;
  int64_t start_time;
  // constructors
  CallContext() { start_time = CurrentTimeMicros(); }

  CallContext(JNIEnv *env, jobject callback_obj, jobject data) : timestamps({0}) {
    start_time = CurrentTimeMicros();
    this->callback_instance = env->NewGlobalRef(callback_obj);
    this->data = data != nullptr ? env->NewGlobalRef(data) : nullptr;
  }
};

/**
 * Callback function invoked by C-SDK to complete an operation execution.
 *
 * The method attaches the VM context to the thread first if necessary because
 * the original app thread may have been removed already, and also because the
 * C-SDK may run the callback from a different thread than the original.
 *
 * Then, the method resolves the callback method AsyncMethodCall.onCompletion()
 * from the call context (All direct connection C-SDK operations use the same
 * callback method), and invokes it. Then handle error, release resources, and
 * exit.
 */
static void HandleCompletionGeneric(status_code_t status_code, completion_data_t *completion_data,
                                    payload_t response, void *cb_args) {
  CallContext *call_context = reinterpret_cast<CallContext *>(cb_args);

  DEBUG_LOG("HandleCompletionGeneric() -- start");
  JNIEnv *env;
  int get_env_stat =
      jni_config->jvm()->GetEnv(reinterpret_cast<void **>(&env), jni_config->jni_version());
  // Attach the thread to JVM if necessary
  if (get_env_stat == JNI_EDETACHED) {
    DEBUG_LOG("GetEnv: not attached");
    if (jni_config->jvm()->AttachCurrentThread(reinterpret_cast<void **>(&env), NULL) != 0) {
      fprintf(stderr, "CSDK ERROR: Failed to attach callback thread");
      return;
    }
    DEBUG_LOG("env=%p", env);
  } else if (get_env_stat == JNI_OK) {
    DEBUG_LOG("attached already");
  } else if (get_env_stat == JNI_EVERSION) {
    fprintf(stderr,
            "CSDK ERROR: JNI version mismatch happened during "
            "attaching callback thread");
    return;
  }

  // Execute the callback
  jstring ep = completion_data->endpoint.length > 0
      ? env->NewStringUTF(completion_data->endpoint.text) : nullptr;
  jobject buf =
      response.length > 0 ? env->NewDirectByteBuffer(response.data, response.length) : nullptr;

  int64_t csdk_latency = CurrentTimeMicros() - call_context->start_time;
  // int64_t num_reads = call_context->num_reads;
  // int64_t num_writes = call_context->num_writes;
  int64_t qos_retry_considered = completion_data->qos_retry_considered ? 1 : 0;
  int64_t qos_retry_sent = completion_data->qos_retry_sent ? 1 : 0;
  int64_t qos_retry_response_used = completion_data->qos_retry_response_used ? 1 : 0;

  env->CallStaticVoidMethod(jni_config->callback_center(), jni_config->on_completion(),
                            call_context->callback_instance, status_code, ep, buf, response.length,
                            call_context->timestamps.length, csdk_latency, qos_retry_considered,
                            qos_retry_sent, qos_retry_response_used);
  if (env->ExceptionCheck()) {
    env->ExceptionDescribe();
  }
  DEBUG_LOG("HandleCompletionGeneric replied");

  // Tear down
  env->DeleteGlobalRef(call_context->callback_instance);
  env->DeleteGlobalRef(call_context->data);
  delete call_context;
  // detach thread if attaching happened in this method.
  if (get_env_stat == JNI_EDETACHED) {
    jni_config->jvm()->DetachCurrentThread();
  }
  DEBUG_LOG("HandleCompletionGeneric() -- end");
}

static void AskIngestBulk(int64_t bulk_ingest_ctx, int from_index, int to_index, void *cb_args) {
  CallContext *call_context = reinterpret_cast<CallContext *>(cb_args);

  DEBUG_LOG("AskIngestBulk() -- start");
  JNIEnv *env;
  int get_env_stat =
      jni_config->jvm()->GetEnv(reinterpret_cast<void **>(&env), jni_config->jni_version());
  // Attach the thread to JVM if necessary
  if (get_env_stat == JNI_EDETACHED) {
    DEBUG_LOG("GetEnv: not attached");
    if (jni_config->jvm()->AttachCurrentThread(reinterpret_cast<void **>(&env), NULL) != 0) {
      fprintf(stderr, "CSDK ERROR: Failed to attach callback thread");
      return;
    }
    DEBUG_LOG("env=%p", env);
  } else if (get_env_stat == JNI_OK) {
    DEBUG_LOG("attached already");
  } else if (get_env_stat == JNI_EVERSION) {
    fprintf(stderr,
            "CSDK ERROR: JNI version mismatch happened during "
            "attaching callback thread");
    return;
  }

  env->CallStaticVoidMethod(jni_config->callback_center(), jni_config->more_ingest_bulk(),
                            call_context->callback_instance, bulk_ingest_ctx, from_index, to_index);
  if (env->ExceptionCheck()) {
    env->ExceptionDescribe();
  }
  DEBUG_LOG("AskIngestBulk() -- executed");
}

/*
 * Class:     io_isima_bios_sdk_csdk_CSdkDirect
 * Method:    startSession
 * Signature: (Ljava/lang/String;IZLjava/lang/String;Ljava/lang/String;
 *             JLio/isima/bios/sdk/csdk/CSdkApiCaller;)V
 */
JNIEXPORT void JNICALL Java_io_isima_bios_sdk_csdk_CSdkDirect_startSession(
    JNIEnv *env, jobject obj, jstring host, jint port, jboolean ssl_enabled, jstring journal_dir,
    jstring cert_file, jlong operationTimeoutMillis, jobject callback_obj) {
  DEBUG_LOG("JNI startSession() -- start");
  CallContext *call_context = new CallContext(env, callback_obj, nullptr);

  string_t host_api = MakeString(env, host);
  string_t journal_dir_api = MakeString(env, journal_dir);
  string_t cert_file_api = MakeString(env, cert_file);
  DaStartSession(jni_config->capi(), host_api, port, ssl_enabled, journal_dir_api, cert_file_api,
                 operationTimeoutMillis, HandleCompletionGeneric, call_context);
  ReleaseString(env, host, host_api);
  ReleaseString(env, journal_dir, journal_dir_api);
  ReleaseString(env, cert_file, cert_file_api);
}

/*
 * Class:     io_isima_bios_sdk_csdk_CSdkDirect
 * Method:    loginBios
 * Signature: (ILjava/nio/ByteBuffer;JLio/isima/bios/sdk/csdk/CSdkApiCaller;)V
 */

void JNICALL Java_io_isima_bios_sdk_csdk_CSdkDirect_loginBios(JNIEnv *env, jobject obj,
                                                              jint session_id, jobject data,
                                                              jlong length, jobject callback_obj) {
  DEBUG_LOG("JNI login_bios() -- start");
  payload_t payload;
  payload.data = reinterpret_cast<uint8_t *>(env->GetDirectBufferAddress(data));
  payload.length = length;

  CallContext *call_context = new CallContext(env, callback_obj, data);

  int disable_routing = 0;  // Java SDK does not support this
  DaLoginBIOS(jni_config->capi(), session_id, payload, disable_routing, HandleCompletionGeneric,
              call_context);
  DEBUG_LOG("JNI login_bios() -- end");
}

/*
 * Class:     io_isima_bios_sdk_csdk_CSdkDirect
 * Method:    simpleMethod
 * Signature: (IILjava/lang/String;Ljava/lang/String;Ljava/nio/ByteBuffer;
 *             JLio/isima/bios/sdk/csdk/CSdkApiCaller;Ljava/nio/ByteBuffer;)V
 */
void JNICALL Java_io_isima_bios_sdk_csdk_CSdkDirect_simpleMethod(
    JNIEnv *env, jobject obj, jint session_id, jint op_id, jstring tenant_name, jstring stream_name,
    jobject data, jlong length, jobject callback_obj, jobject timestamps_obj) {
  int64_t dispatched = GetTimestamp();

  DEBUG_LOG("JNI simpleMethod() -- start");
  payload_t payload;
  if (data != nullptr) {
    payload.data = reinterpret_cast<uint8_t *>(env->GetDirectBufferAddress(data));
  } else {
    payload.data = nullptr;
  }
  payload.length = length;

  CallContext *call_context = new CallContext(env, callback_obj, data);

  payload_t *timestamps;
  if (timestamps_obj != nullptr) {
    timestamps = &call_context->timestamps;
    timestamps->data = reinterpret_cast<uint8_t *>(env->GetDirectBufferAddress(timestamps_obj));
    int64_t *timestamp = reinterpret_cast<int64_t *>(timestamps->data);
    *timestamp = dispatched;
    timestamps->length = sizeof(*timestamp);
  } else {
    timestamps = nullptr;
  }

  string_t tenant_name_api = MakeString(env, tenant_name);
  string_t stream_name_api = MakeString(env, stream_name);
  DaSimpleMethod(jni_config->capi(), session_id, static_cast<CSdkOperationId>(op_id),
                 tenant_name != nullptr ? &tenant_name_api : nullptr,
                 stream_name != nullptr ? &stream_name_api : nullptr, payload, timestamps, nullptr,
                 HandleCompletionGeneric, call_context);
  ReleaseString(env, tenant_name, tenant_name_api);
  ReleaseString(env, stream_name, stream_name_api);

  DEBUG_LOG("JNI simpleMethod() -- end");
}

/*
 * Class:     io_isima_bios_sdk_csdk_CSdkDirect
 * Method:    genericMethod
 * Signature: (II[Ljava/lang/String;[Ljava/lang/String;Ljava/nio/ByteBuffer;
 *             JLio/isima/bios/sdk/csdk/CSdkApiCaller;Ljava/nio/ByteBuffer;)V
 */
void JNICALL Java_io_isima_bios_sdk_csdk_CSdkDirect_genericMethod(
    JNIEnv *env, jobject obj, jint session_id, jint op_id, jobjectArray resourceArray,
    jobjectArray optionArray, jobject data, jlong length, jobject callback_obj,
    jobject timestamps_obj) {
  int64_t dispatched = GetTimestamp();

  DEBUG_LOG("JNI genericMethod() -- start");
  payload_t payload;
  if (data != nullptr) {
    payload.data = reinterpret_cast<uint8_t *>(env->GetDirectBufferAddress(data));
  } else {
    payload.data = nullptr;
  }
  payload.length = length;

  CallContext *call_context = new CallContext(env, callback_obj, data);

  payload_t *timestamps;
  if (timestamps_obj != nullptr) {
    timestamps = &call_context->timestamps;
    timestamps->data = reinterpret_cast<uint8_t *>(env->GetDirectBufferAddress(timestamps_obj));
    int64_t *timestamp = reinterpret_cast<int64_t *>(timestamps->data);
    *timestamp = dispatched;
    timestamps->length = sizeof(*timestamp);
  } else {
    timestamps = nullptr;
  }

  int num_resources = (resourceArray == nullptr) ? 0 : env->GetArrayLength(resourceArray);
  assert(num_resources < kMaxTempArraySize);
  string_t resources[kMaxTempArraySize];
  for (int i = 0; i < num_resources; ++i) {
    resources[i] = MakeString(env, (jstring)env->GetObjectArrayElement(resourceArray, i));
  }
  resources[num_resources] = {0};

  int num_options = (optionArray == nullptr) ? 0 : env->GetArrayLength(optionArray);
  assert(num_options < kMaxTempArraySize);
  string_t options[kMaxTempArraySize];
  for (int i = 0; i < num_options; ++i) {
    options[i] = MakeString(env, (jstring)env->GetObjectArrayElement(optionArray, i));
  }
  options[num_options] = {0};

  DaGenericMethod(jni_config->capi(), session_id, static_cast<CSdkOperationId>(op_id),
                  num_resources > 0 ? resources : nullptr, num_options > 0 ? options : nullptr,
                  payload, -1, timestamps, HandleCompletionGeneric, call_context);

  for (int i = 0; i < num_resources; ++i) {
    ReleaseString(env, (jstring)env->GetObjectArrayElement(resourceArray, i), resources[i]);
  }

  for (int i = 0; i < num_options; ++i) {
    ReleaseString(env, (jstring)env->GetObjectArrayElement(optionArray, i), options[i]);
  }

  DEBUG_LOG("JNI genericMethod() -- end");
}

/*
 * Class:     io_isima_bios_sdk_csdk_CSdkDirect
 * Method:    ingestBulkStart
 * Signature: (IIILio/isima/bios/sdk/csdk/InsertBulkExecutor;)J
 */
JNIEXPORT jlong JNICALL Java_io_isima_bios_sdk_csdk_CSdkDirect_ingestBulkStart(
    JNIEnv *env, jobject obj, jint session_id, jint op_id, jint size,
    jobject bulk_ingest_call_obj) {
  CallContext *call_context = new CallContext(env, bulk_ingest_call_obj, nullptr);
  DaIngestBulkStart(jni_config->capi(), session_id, static_cast<CSdkOperationId>(op_id), size,
                    HandleCompletionGeneric, AskIngestBulk, call_context);
  DEBUG_LOG("ingestBulkStart -- call_context=%p", call_context);
  return reinterpret_cast<int64_t>(call_context);
}

/*
 * Class:     io_isima_bios_sdk_csdk_CSdkDirect
 * Method:    ingestBulk
 * Signature:
 (IJII[Ljava/lang/String;Ljava/nio/ByteBuffer;JLio/isima/bios/sdk/csdk/CSdkApiCaller;
               Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_io_isima_bios_sdk_csdk_CSdkDirect_ingestBulk(
    JNIEnv *env, jobject obj, jint session_id, jlong bulk_ingest_ctx, jint from_index,
    jint to_index, jobjectArray optionArray, jobject data, jlong length, jobject callback_obj,
    jobject timestamps_obj) {
  int64_t dispatched = GetTimestamp();
  DEBUG_LOG("JNI ingestBulk() -- start");

  int num_options = (optionArray == nullptr) ? 0 : env->GetArrayLength(optionArray);
  assert(num_options < kMaxTempArraySize);
  string_t options[kMaxTempArraySize];
  for (int i = 0; i < num_options; ++i) {
    options[i] = MakeString(env, (jstring)env->GetObjectArrayElement(optionArray, i));
  }
  options[num_options] = {0};

  payload_t payload;
  if (data != nullptr) {
    payload.data = reinterpret_cast<uint8_t *>(env->GetDirectBufferAddress(data));
  } else {
    payload.data = nullptr;
  }
  payload.length = length;

  CallContext *call_context = new CallContext(env, callback_obj, data);

  payload_t *timestamps;
  if (timestamps_obj != nullptr) {
    timestamps = &call_context->timestamps;
    timestamps->data = reinterpret_cast<uint8_t *>(env->GetDirectBufferAddress(timestamps_obj));
    int64_t *timestamp = reinterpret_cast<int64_t *>(timestamps->data);
    *timestamp = dispatched;
    timestamps->length = sizeof(*timestamp);
  } else {
    timestamps = nullptr;
  }

  DaIngestBulk(jni_config->capi(), session_id, bulk_ingest_ctx, from_index, to_index,
               nullptr, options, payload, timestamps, HandleCompletionGeneric, call_context);

  DEBUG_LOG("JNI ingestBulk() -- end");
}

/*
 * Class:     io_isima_bios_sdk_csdk_CSdkDirect
 * Method:    ingestBulkEnd
 * Signature: (IJ)V
 */
JNIEXPORT void JNICALL Java_io_isima_bios_sdk_csdk_CSdkDirect_ingestBulkEnd(
    JNIEnv *env, jobject obj, jlong call_context_id, jint session_id, jlong bulk_ingest_ctx) {
  CallContext *call_context = reinterpret_cast<CallContext *>(call_context_id);
  DaIngestBulkEnd(jni_config->capi(), session_id, bulk_ingest_ctx);
  // Tear down
  env->DeleteGlobalRef(call_context->callback_instance);
  env->DeleteGlobalRef(call_context->data);
  delete call_context;
}

static void HandleOperationPermissionError(status_code_t status_code,
                                           completion_data_t *completion_data,
                                           payload_t payload, void *cb_args) {
  auto env = reinterpret_cast<JNIEnv *>(cb_args);
  // Just in case, do nothing if the callback receives "no error"
  if (status_code == 0) {
    return;
  }
  DEBUG_LOG("HandleOperationPermissionError invoked status_code=%x", status_code);
  auto cls = env->FindClass("io/isima/bios/sdk/csdk/CSdkException");
  if (cls == nullptr) {
    return;
  }
  auto constructor = env->GetMethodID(cls, "<init>", "(ILjava/lang/String;)V");
  if (constructor == nullptr) {
    return;
  }
  DEBUG_LOG("HandleOperationPermissionError -- Class CSdkException resolved");
  jstring message =
      payload.length > 0 ? env->NewStringUTF(reinterpret_cast<char *>(payload.data)) : nullptr;
  jthrowable exception = (jthrowable)env->NewObject(cls, constructor, status_code, message);
  env->Throw(exception);
}

/*
 * Class:     io_isima_bios_sdk_csdk_CSdkDirect
 * Method:    checkOperationPermissionNative
 * Signature: (II)V
 */
JNIEXPORT void JNICALL Java_io_isima_bios_sdk_csdk_CSdkDirect_checkOperationPermissionNative(
    JNIEnv *env, jobject, jint session_id, jint op_id) {
  DaCheckOperationPermission(jni_config->capi(), session_id, static_cast<CSdkOperationId>(op_id),
                             HandleOperationPermissionError, env);
}

/*
 * Class:     io_isima_bios_sdk_csdk_CSdkDirect
 * Method:    endSession
 * Signature: (I)V
 */
JNIEXPORT void JNICALL Java_io_isima_bios_sdk_csdk_CSdkDirect_endSession(JNIEnv *, jobject,
                                                                         jint session_id) {
  DaCloseSession(jni_config->capi(), session_id);
}

/*
 * Class:     io_isima_bios_sdk_csdk_CSdkDirect
 * Method:    allocateBuffer
 * Signature: (I)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_io_isima_bios_sdk_csdk_CSdkDirect_allocateBuffer(JNIEnv *env,
                                                                                jobject,
                                                                                jint capacity) {
  payload_t buffer = CsdkAllocatePayload(capacity);
  jobject buf = env->NewDirectByteBuffer(buffer.data, buffer.length);
  return buf;
}

/*
 * Class:     io_isima_bios_sdk_csdk_CSdkDirect
 * Method:    releasePayload
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_io_isima_bios_sdk_csdk_CSdkDirect_releasePayload(JNIEnv *env, jobject,
                                                                             jobject buffer) {
  if (buffer == nullptr) {
    // do nothing in this case
    return;
  }
  payload_t payload;
  payload.data = reinterpret_cast<uint8_t *>(env->GetDirectBufferAddress(buffer));
  payload.length = env->GetDirectBufferCapacity(buffer);
  CsdkReleasePayload(&payload);
}

/*
 * Class:     io_isima_bios_sdk_csdk_CSdkDirect
 * Method:    log
 * Signature: (ILjava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;JJJJJJJ)V
 */
JNIEXPORT void JNICALL Java_io_isima_bios_sdk_csdk_CSdkDirect_log(
    JNIEnv *env, jobject obj, jint session_id, jstring stream, jstring request, jstring status,
    jstring server_endpoint, jlong latency_us, jlong latency_internal_us, jlong num_reads,
    jlong num_writes, jlong qos_retry_considered, jlong qos_retry_sent,
    jlong qos_retry_response_used) {
  const char *stream_api = env->GetStringUTFChars(stream, nullptr);
  const char *request_api = env->GetStringUTFChars(request, nullptr);
  const char *status_api = env->GetStringUTFChars(status, nullptr);
  const char *server_endpoint_api = env->GetStringUTFChars(server_endpoint, nullptr);

  DaLog(jni_config->capi(), session_id, stream_api, request_api, status_api, server_endpoint_api,
        latency_us, latency_internal_us, num_reads, num_writes, qos_retry_considered,
        qos_retry_sent, qos_retry_response_used);

  env->ReleaseStringUTFChars(stream, stream_api);
  env->ReleaseStringUTFChars(request, request_api);
  env->ReleaseStringUTFChars(status, status_api);
  env->ReleaseStringUTFChars(server_endpoint, server_endpoint_api);
}

/*
 * Class:     io_isima_bios_sdk_csdk_CSdkDirect
 * Method:    currentTimeMillis
 * Signature: ()J
 */
jlong JNICALL Java_io_isima_bios_sdk_csdk_CSdkDirect_currentTimeMillis(JNIEnv *, jobject) {
  return GetTimestamp();
}

/*
 * Class:     io_isima_bios_sdk_csdk_CSdkDirect
 * Method:    getOperationName
 * Signature: (I)Ljava/lang/String;
 */
jstring JNICALL Java_io_isima_bios_sdk_csdk_CSdkDirect_getOperationName(JNIEnv *env, jobject obj,
                                                                        jint id) {
  string_t name = CsdkGetOperationName(static_cast<CSdkOperationId>(id));
  jstring reply = env->NewStringUTF(name.text);
  return reply;
}

/*
 * Class:     io_isima_bios_sdk_csdk_CSdkDirect
 * Method:    listStatusCodesNative
 * Signature: ()[I
 */
JNIEXPORT jintArray JNICALL
Java_io_isima_bios_sdk_csdk_CSdkDirect_listStatusCodesNative(JNIEnv *env, jobject obj) {
  payload_t data = CsdkListStatusCodes();
  jintArray result;
  int num_elements = data.length / sizeof(status_code_t);
  result = env->NewIntArray(num_elements);
  if (result == nullptr) {
    return nullptr;
  }
  for (int i = 0; i < num_elements; ++i) {
    jint status_code;
    status_code = *reinterpret_cast<status_code_t *>(&data.data[i * sizeof(status_code_t)]);
    env->SetIntArrayRegion(result, i, 1, &status_code);
  }
  CsdkReleasePayload(&data);
  return result;
}

/*
 * Class:     io_isima_bios_sdk_csdk_CSdkDirect
 * Method:    getStatusName
 * Signature: (I)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_io_isima_bios_sdk_csdk_CSdkDirect_getStatusName(JNIEnv *env,
                                                                               jobject obj,
                                                                               jint status) {
  string_t name = CsdkGetStatusName(status);
  jstring reply = env->NewStringUTF(name.text);
  return reply;
}

/*
 * Class:     io_isima_bios_sdk_csdk_CSdkDirect
 * Method:    writeHello
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT jint JNICALL Java_io_isima_bios_sdk_csdk_CSdkDirect_writeHelloNative(JNIEnv *env, jobject,
                                                                               jobject buffer) {
  payload_t payload;
  payload.data = reinterpret_cast<uint8_t *>(env->GetDirectBufferAddress(buffer));
  payload.length = env->GetDirectBufferCapacity(buffer);
  return CsdkWriteHello(&payload);
}

// utilities ///////////////////////////////////////////////////
static string_t MakeString(JNIEnv *env, jstring src) {
  if (src == nullptr) {
    return {.text = nullptr, .length = 0};
  }
  const char *text = env->GetStringUTFChars(src, nullptr);
  jsize length = env->GetStringUTFLength(src);
  return {
      .text = text,
      .length = length,
  };
}

void ReleaseString(JNIEnv *env, jstring src, const string_t &to_release) {
  if (src != nullptr) {
    env->ReleaseStringUTFChars(src, to_release.text);
  }
}
