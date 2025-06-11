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
#ifndef NATIVE_PARQUET_WRITER_STORE_H_
#define NATIVE_PARQUET_WRITER_STORE_H_

#include <unordered_map>
#include <mutex>
#include <atomic>
#include <memory>

#include "parquet/parquet_common.h"
#include "parquet/parquet_writer.h"

namespace bios::parquetjni {

typedef int64_t writer_handle_t;

class WriterStore {
 public:
  WriterStore();
  ~WriterStore() = default;
  DISABLE_COPY_MOVE_AND_ASSIGN(WriterStore);

  writer_handle_t CreateParquetWriter(std::shared_ptr<Schema>& schema);
  ParquetWriter *GetParquetWriter(writer_handle_t writer_handle);
  void RemoveParquetWriter(writer_handle_t writer_handle);

 private:
  std::unordered_map<writer_handle_t, std::unique_ptr<ParquetWriter>> parquet_writer_store_;
  long current_writer_id_;
  std::mutex store_mutex_;
};

}  // namespace bios::parquetjni

#endif //NATIVE_PARQUET_WRITER_STORE_H_
