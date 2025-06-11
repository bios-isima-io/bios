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
#include <iostream>
#include <utility>

#include "parquet/writer_store.h"

namespace bios::parquetjni {

WriterStore::WriterStore()
    : parquet_writer_store_{}, current_writer_id_(1), store_mutex_{} {}

writer_handle_t WriterStore::CreateParquetWriter(std::shared_ptr<arrow::Schema> &schema) {
  std::lock_guard<std::mutex> lock(store_mutex_);
  writer_handle_t next = current_writer_id_++;
  auto writer = std::make_unique<ParquetWriter>(schema);
  auto ret = writer->Init();
  if (!ret.ok()) {
    std::cerr << "Create Parquet Writer failed with: "
              << ret
              << std::endl;
    return -1;
  }
  parquet_writer_store_[next] = std::move(writer);
  return next;
}

ParquetWriter *WriterStore::GetParquetWriter(writer_handle_t writer_handle) {
  assert(writer_handle > 0);

  std::lock_guard<std::mutex> lock(store_mutex_);
  auto p = parquet_writer_store_.find(writer_handle);
  if (p != parquet_writer_store_.end()) {
    return p->second.get();
  } else {
    return nullptr;
  }
}

void WriterStore::RemoveParquetWriter(writer_handle_t writer_handle) {
  assert(writer_handle > 0);

  std::lock_guard<std::mutex> lock(store_mutex_);

  auto itr = parquet_writer_store_.find(writer_handle);
  if (itr != parquet_writer_store_.end()) {
    parquet_writer_store_.erase(itr);
  }
}

}  // namespace bios::parquetjni
