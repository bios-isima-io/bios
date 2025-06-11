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

#include <memory>
#include <iostream>

#include "parquet/stream_creator.h"

namespace bios::parquetjni {

using arrow::io::BufferOutputStream;
using arrow::Result;

MemoryStreamCreator::MemoryStreamCreator() :
  mem_writer_(std::shared_ptr<BufferOutputStream>(nullptr)) {
}

MemoryStreamCreator::~MemoryStreamCreator() {
  Close();
}

void MemoryStreamCreator::Close() {
  auto mem_w = mem_writer_.get();
  if (mem_w) {
    auto status = mem_w->Finish();
  }
}

Status MemoryStreamCreator::OpenForWrite() {
  Result<std::shared_ptr<BufferOutputStream>> res;
  res = BufferOutputStream::Create();
  if (res.ok()) {
    mem_writer_ = res.ValueOrDie();
  } else {
    std::cerr << "Writable Memory Stream Creation failed with: " << res.status() << std::endl;
  }
  return res.status();
}

Result<std::shared_ptr<Buffer>> MemoryStreamCreator::Finish() {
  auto mem_w = mem_writer_.get();
  if (mem_w) {
    auto cls = mem_w->Close();
    auto ret = mem_w->Finish();
    mem_writer_ = std::shared_ptr<BufferOutputStream>(nullptr);
    return ret;
  }
  return Status::Invalid("Memory stream already finished");
}

}  // namespace bios::parquetjni
