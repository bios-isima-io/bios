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
#include "parquet/parquet_writer.h"

namespace bios::parquetjni {

using arrow::ArrayDataVector;
using arrow::Table;
using arrow::RecordBatch;
using arrow::Result;
using arrow::Status;
using arrow::Schema;
using arrow::MemoryPool;
using arrow::Buffer;

ParquetWriter::ParquetWriter(std::shared_ptr<Schema> &schema) :
    schema_(schema),
    schema_desc_(std::shared_ptr<parquet::SchemaDescriptor>(nullptr)),
    stream_creator_(std::make_unique<MemoryStreamCreator>()),
    parquet_writer_(std::unique_ptr<parquet::arrow::FileWriter>(nullptr)),
    record_batches_{},
    mem_pool_(default_memory_pool()),
    writer_mutex_{},
    finished_buffer_(std::shared_ptr<arrow::Buffer>(nullptr)) {}

ParquetWriter::~ParquetWriter() {
  Status msg = parquet_writer_->Close();
  if (!msg.ok()) {
    std::cerr << "Closing ParquetFileWriter Failed with: " << msg << std::endl;
  }
}

Status ParquetWriter::Init() {
  Status status = stream_creator_->OpenForWrite();
  const std::shared_ptr<parquet::WriterProperties>& props = parquet::default_writer_properties();
  Status msg = parquet::arrow::ToParquetSchema(schema_.get(), *props.get(), &schema_desc_);
  if (!msg.ok()) {
    std::cerr << "Arrow Schema to Parquet Schema conversion failed with: " << msg << std::endl;
    return msg;
  }

  parquet::schema::NodeVector gn_fields;
  for (int i = 0; i < schema_desc_->group_node()->field_count(); i++) {
    gn_fields.push_back(schema_desc_->group_node()->field(i));
  }
  auto parquet_schema = std::static_pointer_cast<parquet::schema::GroupNode>(
      parquet::schema::GroupNode::Make(
          schema_desc_->schema_root()->name(),
          schema_desc_->schema_root()->repetition(),
          gn_fields));

  msg = parquet::arrow::FileWriter::Make(
      mem_pool_,
      parquet::ParquetFileWriter::Open(stream_creator_->GetWriter(), parquet_schema),
      schema_,
      parquet::default_arrow_writer_properties(),
      &parquet_writer_);
  if (!msg.ok()) {
    std::cerr << "Making parquet writer failed with: " << msg << std::endl;
    return msg;
  }
  return Status::OK();
}

Status ParquetWriter::WriteNext(int num_rows, int64_t *buf_addrs, int64_t *buf_sizes,
    int bufs_len) {
  auto res = BuildRecordBatch(num_rows, buf_addrs, buf_sizes, bufs_len);
  if (!res.ok()) {
    return res.status();
  }

  std::lock_guard<std::mutex> lock(writer_mutex_);
  record_batches_.push_back(res.ValueOrDie());

  return res.status();
}

Status ParquetWriter::Flush() {
  auto res = Table::FromRecordBatches(record_batches_);
  if (!res.ok()) {
    std::cerr
      << "Arrow table creation from record batches failed with: "
      << res.status()
      << std::endl;
    return res.status();
  }
  auto table = res.ValueOrDie();
  if (table->num_rows() > 0) {
    DEBUG_LOG("Write Parquet Table: Num Rows = %ld", (int64_t) table->num_rows());
    auto msg = parquet_writer_->WriteTable(*(table.get()), table->num_rows());
    if (!msg.ok()) {
      std::cerr << "Arrow table to parquet flush failed" << std::endl;
      return msg;
    }
    DEBUG_LOG("Write table complete");
  }
  record_batches_.clear();

  auto msg = stream_creator_->GetWriter()->Flush();
  return msg;
}

buf_t ParquetWriter::Finish() {
  Result<std::shared_ptr<arrow::Buffer>> ret;
  if (finished_buffer_ == nullptr) {
    auto cls = parquet_writer_->Close();
    ret = stream_creator_->Finish();
    if (ret.ok()) {
      finished_buffer_ = ret.ValueOrDie();
    } else {
      std::cerr << "Error in obtaining transformed parquet buffer: " << ret.status() << std::endl;
      return buf {.data = nullptr, .length = 0};
    }
  }
  auto addr = const_cast<uint8_t *>(finished_buffer_->data());
  DEBUG_LOG("Finish Buffer %ld", (int64_t) finished_buffer_->size());
  return buf {.data = addr, .length = finished_buffer_->size()};
}

Result<std::shared_ptr<arrow::RecordBatch>> ParquetWriter::BuildRecordBatch(
    int num_rows, const int64_t *buf_addrs, const int64_t *buf_sizes, int buf_len) {
  std::vector<std::shared_ptr<ArrayData>> arrays;
  auto num_fields = schema_->num_fields();
  int buf_idx = 0;

  DEBUG_LOG("Build Record Batch: %d", buf_len);
  for (int i = 0; i < num_fields; i++) {
    assert(buf_idx < buf_len);

    auto field = schema_->field(i);
    DEBUG_LOG("FIELD %d: = %s;%d", i, field->ToString(true).c_str(), buf_idx);

    std::vector<std::shared_ptr<Buffer>> buffers;

    auto validity_addr = buf_addrs[buf_idx];
    auto validity_size = buf_sizes[buf_idx++];
    DEBUG_LOG("VALIDITY -> sz = %ld: addr = %p", validity_size, (void *)validity_addr);
    auto validity = std::make_shared<Buffer>(
        reinterpret_cast<uint8_t *>(validity_addr), validity_size);
    buffers.push_back(validity);
    assert(buf_idx < buf_len);

    auto value_addr = buf_addrs[buf_idx];
    auto value_size = buf_sizes[buf_idx++];
    DEBUG_LOG("VALUE -> sz = %ld: addr = %p", value_size, (void *)value_addr);
    auto data = std::make_shared<Buffer>(reinterpret_cast<uint8_t *>(value_addr), value_size);
    buffers.push_back(data);

    if (arrow::is_binary_like(field->type()->id())) {
      assert(buf_idx < buf_len);

      auto offsets_addr = buf_addrs[buf_idx];
      auto offsets_size = buf_sizes[buf_idx++];
      DEBUG_LOG("OFFSETS -> sz = %ld: addr = %p", offsets_size, (void *)offsets_addr);
      auto offsets = std::make_shared<Buffer>(
          reinterpret_cast<uint8_t *>(offsets_addr), offsets_size);
      buffers.push_back(offsets);
    }

    auto array_data = arrow::ArrayData::Make(field->type(), num_rows, std::move(buffers));
    arrays.push_back(array_data);
  }
  return arrow::RecordBatch::Make(schema_, num_rows, arrays);
}

}  // namespace bios::parquetjni
