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
#include <jni.h>
#include <string>
#include <iostream>
#include <memory>
#include <cinttypes>

#include "arrow/ipc/api.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/buffer.h"

#include "parquet/jni_arrow_config.h"

static bios::parquetjni::JniArrowConfig *arrow_config = nullptr;

#ifdef __cplusplus
extern "C" {
#endif

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  arrow_config = new bios::parquetjni::JniArrowConfig(vm);
  return arrow_config->GetJniVersion();
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  std::cerr << "JNI_OnUnload" << std::endl;
  if (arrow_config == nullptr) {
    return;
  }
  delete arrow_config;
  arrow_config = nullptr;
}

/*
 * Class:     io_isima_bios_parquet_ParquetJniWrapper
 * Method:    openParquetWriter
 * Signature: ([B)J
 */
JNIEXPORT jlong JNICALL Java_io_isima_bios_parquet_ParquetJniWrapper_openParquetWriter
  (JNIEnv *env, jobject this_obj, jbyteArray schema_bytes) {
  if (arrow_config == nullptr) {
    std::cerr << "Open Parquet Writer: JNI not initialized " << std::endl;
    return -1;
  }

  int schema_len = env->GetArrayLength(schema_bytes);
  jbyte* schema_data = env->GetByteArrayElements(schema_bytes, 0);

  auto schema_buf = std::make_shared<arrow::Buffer>(
      reinterpret_cast<uint8_t *>(schema_data), schema_len);

  arrow::ipc::DictionaryMemo memo;
  arrow::io::BufferReader buf_reader(schema_buf);

  auto schema_res = arrow::ipc::ReadSchema(&buf_reader, &memo);
  if (!schema_res.ok()) {
    std::cerr << "Open Parquet Writer: Read schema failed with: "
              << schema_res.status()
              << std::endl;
    return -1;
  }
  auto schema = schema_res.ValueOrDie();

  bios::parquetjni::WriterStore *writer_store = arrow_config->GetWriterStore();
  bios::parquetjni::writer_handle_t writer_handle = writer_store->CreateParquetWriter(schema);
  env->ReleaseByteArrayElements(schema_bytes, schema_data, JNI_ABORT);
  return writer_handle;
}

/*
 * Class:     io_isima_bios_parquet_ParquetJniWrapper
 * Method:    closeParquetWriter
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_io_isima_bios_parquet_ParquetJniWrapper_destroyParquetWriter(
    JNIEnv *env, jobject this_obj, jlong writer_handle) {
  if (arrow_config == nullptr) {
    std::cerr << "Destroy Parquet Writer: JNI not initialized " << std::endl;
    return;
  }
  bios::parquetjni::WriterStore *writer_store = arrow_config->GetWriterStore();
  bios::parquetjni::ParquetWriter *writer = writer_store->GetParquetWriter(writer_handle);
  if (writer == nullptr) {
    std::cerr << "Destroy Parquet Writer: Writer already closed" << std::endl;
    return;
  }
  writer_store->RemoveParquetWriter(writer_handle);
  return;
}

/*
 * Class:     io_isima_bios_parquet_ParquetJniWrapper
 * Method:    writeNext
 * Signature: (JI[J[J)V
 */
JNIEXPORT void JNICALL Java_io_isima_bios_parquet_ParquetJniWrapper_writeNext
  (JNIEnv *env, jobject this_obj, jlong writer_handle, jint num_rows,
      jlongArray buf_addrs, jlongArray buf_sizes) {
  if (arrow_config == nullptr) {
    std::cerr << "Parquet Write Batch: JNI not initialized " << std::endl;
    return;
  }

  int ba_len = env->GetArrayLength(buf_addrs);
  int bs_len = env->GetArrayLength(buf_sizes);
  if (ba_len != bs_len) {
    std::cerr << "Parquet Write Batch: Invalid arguments " << std::endl;
    return;
  }

  bios::parquetjni::WriterStore *writer_store = arrow_config->GetWriterStore();
  bios::parquetjni::ParquetWriter *writer = writer_store->GetParquetWriter(writer_handle);
  if (writer == nullptr) {
    std::cerr << "Parquet Write Batch: Writer already closed" << std::endl;
    return;
  }
  jlong* ptr_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong* ptr_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);
  arrow::Status status = writer->WriteNext(num_rows, ptr_buf_addrs, ptr_buf_sizes, ba_len);
  if (!status.ok()) {
    std::cerr << "Parquet write batch: Write batch failed with: " << status << std::endl;
    return;
  }
  env->ReleaseLongArrayElements(buf_addrs, ptr_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, ptr_buf_sizes, JNI_ABORT);
}

/*
 * Class:     io_isima_bios_parquet_ParquetJniWrapper
 * Method:    getParquetDataBuffer
 * Signature: (J)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_io_isima_bios_parquet_ParquetJniWrapper_closeParquetWriter
  (JNIEnv *env, jobject this_obj, jlong writer_handle) {
  if (arrow_config == nullptr) {
    std::cerr << "Parquet Close Writer: JNI not initialized " << std::endl;
    return nullptr;
  }
  bios::parquetjni::WriterStore *writer_store = arrow_config->GetWriterStore();
  bios::parquetjni::ParquetWriter *writer = writer_store->GetParquetWriter(writer_handle);
  if (writer == nullptr) {
    std::cerr << "Parquet Close Writer: Writer destroyed" << std::endl;
    return nullptr;
  }
  DEBUG_LOG("Flushing...%ld", writer_handle);
  arrow::Status msg = writer->Flush();
  if (!msg.ok()) {
    std::cerr << "Flush failed with : " << msg << std::endl;
  }
  DEBUG_LOG("Finishing...%ld", writer_handle);
  auto ret = writer->Finish();
  if (ret.length > 0) {
    DEBUG_LOG("Returning buffer len = %ld, data = %p", (int64_t) ret.length, ret.data);
    jobject direct_buffer = env->NewDirectByteBuffer(ret.data, ret.length);
    return direct_buffer;
  }
  return nullptr;
}

#ifdef __cplusplus
}
#endif
