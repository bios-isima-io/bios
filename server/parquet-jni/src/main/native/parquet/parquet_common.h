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
#ifndef NATIVE_PARQUET_PARQUET_COMMON_H_
#define NATIVE_PARQUET_PARQUET_COMMON_H_
#include <libgen.h>
#include <cinttypes>

#define DISABLE_COPY_MOVE_AND_ASSIGN(TypeName)  \
  TypeName(const TypeName&) = delete;           \
  TypeName(const TypeName&&) = delete;          \
  void operator=(const TypeName&) = delete

#ifndef CACHE_ALIGNMENT_SIZE
#define CACHE_ALIGNMENT_SIZE 64
#endif

// Enable debug print only for debug build.
#if !defined(NDEBUG)
#define DEBUG_LOG_ENABLED
#endif

#define BIOS_DEBUG_LOG_(format, ...) do {                           \
    fprintf(stderr, "%s:%d: " format "\n",                          \
            basename((char *) __FILE__), __LINE__, ##__VA_ARGS__);  \
  } while (0)

#if defined(DEBUG_LOG_ENABLED)
#define DEBUG_LOG(format, ...) BIOS_DEBUG_LOG_(format, ##__VA_ARGS__)
#else
#define DEBUG_LOG(format, ...)
#endif

namespace bios::parquetjni {

typedef struct buf {
  uint8_t *data;
  int64_t length;
} buf_t;


}  // namespace bios::parquetjni


#endif //NATIVE_PARQUET_PARQUET_COMMON_H_
