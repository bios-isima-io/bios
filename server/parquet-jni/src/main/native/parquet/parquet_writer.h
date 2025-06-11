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
#ifndef NATIVE_PARQUET_PARQUET_WRITER_H_
#define NATIVE_PARQUET_PARQUET_WRITER_H_

#include <iostream>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <vector>
#include <utility>

#include <arrow/table.h>
#include <parquet/properties.h>
#include <parquet/file_writer.h>
#include <parquet/arrow/writer.h>
#include <parquet/arrow/schema.h>
#include <arrow/type_traits.h>
#include <arrow/api.h>

#include "parquet/parquet_common.h"
#include "parquet/stream_creator.h"

namespace bios::parquetjni {


class ParquetWriter {
 public:
  explicit ParquetWriter(std::shared_ptr<arrow::Schema> &schema);
  ~ParquetWriter();

  DISABLE_COPY_MOVE_AND_ASSIGN(ParquetWriter);

  Status Init();
  Status WriteNext(int num_rows, int64_t *buf_addrs, int64_t *buf_sizes, int bufs_len);
  Status Flush();
  buf_t Finish();

 private:
  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<::parquet::SchemaDescriptor> schema_desc_;
  std::unique_ptr<StreamCreator> stream_creator_;
  std::unique_ptr<::parquet::arrow::FileWriter> parquet_writer_;
  std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches_;
  arrow::MemoryPool *mem_pool_;
  std::mutex writer_mutex_;
  std::shared_ptr<arrow::Buffer> finished_buffer_;

  Result<std::shared_ptr<arrow::RecordBatch>> BuildRecordBatch(
      int num_rows, const int64_t *buf_addrs, const int64_t *buf_sizes, int buf_len);
};

}

#endif //NATIVE_PARQUET_PARQUET_WRITER_H_
