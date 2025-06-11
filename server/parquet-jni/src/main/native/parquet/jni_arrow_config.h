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
#ifndef NATIVE_PARQUET_JNI_ARROW_CONFIG_H_
#define NATIVE_PARQUET_JNI_ARROW_CONFIG_H_

#include <jni.h>
#include <memory>

#include "parquet_common.h"
#include "writer_store.h"

namespace bios::parquetjni {

class JniArrowConfig {
 private:
  JavaVM *jvm_;
  jint jni_version_;
  std::unique_ptr<WriterStore> writer_store_;

 public:
  const jint JNI_VERSION = JNI_VERSION_1_8;
  JniArrowConfig(JavaVM *vm) : jvm_(vm) {
    JNIEnv* env;
    if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
      jni_version_ = JNI_ERR;
      writer_store_ = std::unique_ptr<WriterStore>(nullptr);
    } else {
      jni_version_ = env->GetVersion();
      writer_store_ = std::make_unique<WriterStore>();
    }
  }
  ~JniArrowConfig() = default;
  DISABLE_COPY_MOVE_AND_ASSIGN(JniArrowConfig);

  inline JavaVM *jvm() const { return jvm_; }
  inline jint GetJniVersion() const { return jni_version_; }
  inline WriterStore *GetWriterStore() const { return writer_store_.get(); }
};

}  // namespace bios::parquetjni

#endif //NATIVE_PARQUET_JNI_ARROW_CONFIG_H_
