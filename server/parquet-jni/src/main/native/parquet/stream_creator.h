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
#ifndef NATIVE_PARQUET_STREAM_CREATOR_H_
#define NATIVE_PARQUET_STREAM_CREATOR_H_

#include <arrow/io/api.h>

#include "parquet/parquet_common.h"

namespace bios::parquetjni {

using namespace ::arrow;
using namespace ::arrow::io;

class StreamCreator {
 public:
  DISABLE_COPY_MOVE_AND_ASSIGN(StreamCreator);
  ~StreamCreator() noexcept = default;

  virtual Status OpenForWrite() = 0;
  virtual std::shared_ptr<OutputStream> GetWriter() = 0;
  virtual void Close() = 0;
  virtual Result<std::shared_ptr<Buffer>> Finish() = 0;

 protected:
  StreamCreator() = default;
};

class MemoryStreamCreator : public StreamCreator {
 public:
  DISABLE_COPY_MOVE_AND_ASSIGN(MemoryStreamCreator);
  MemoryStreamCreator();
  ~MemoryStreamCreator();

  Status OpenForWrite() override;
  std::shared_ptr<OutputStream> GetWriter() override {
    return mem_writer_;
  }
  void Close() override;
  Result<std::shared_ptr<Buffer>> Finish() override;

 private:
  std::shared_ptr<BufferOutputStream> mem_writer_;
};

}  // namespace bios::parquetjni

#endif //NATIVE_PARQUET_STREAM_CREATOR_H_
