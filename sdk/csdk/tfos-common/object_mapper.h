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
#ifndef TFOS_COMMON_OBJECT_MAPPER_H_
#define TFOS_COMMON_OBJECT_MAPPER_H_

#include <rapidjson/document.h>

#include <functional>
#include <memory>
#include <string>
#include <typeindex>
#include <unordered_map>

#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "tfos-common/utils.h"
#include "tfoscsdk/models.h"
#include "tfoscsdk/types.h"

namespace tfos::csdk {

typedef rapidjson::Writer<rapidjson::StringBuffer> JsonWriter;

/**
 * Class that provides means to serialize and deserialize JSON string.
 */
class ObjectMapper {
 public:
  /**
   * Method to deserialize a JSON string to an object of specified type.
   *
   * The method returns nullptr if the specified type is unknown to the mapper.
   * Also, if the JSON string is invalid, the method returns nullptr.
   *
   * @type T Type of the object to be generated
   * @params src JSON data
   * @params length data length
   * @return Parsed instance of the class. The instance must be deleted after
   * being consumed.
   */
  template <typename T>
  std::unique_ptr<T> ReadValue(const uint8_t *src, int64_t length) const {
    if (length == 0) {
      return nullptr;
    }
    auto it = readers.find(std::type_index(typeid(T)));
    rapidjson::Document document;
    document.Parse((char *)src);
    return std::unique_ptr<T>(it != readers.end() ? (T *)it->second(this, &document) : nullptr);
  }

  template <typename T>
  std::unique_ptr<T> ReadValue(const payload_t &src) const {
    return ReadValue<T>(src.data, src.length);
  }

  template <typename T>
  payload_t WriteValue(const T &src) const {
    auto it = writers.find(std::type_index(typeid(T)));
    if (it != writers.end()) {
      rapidjson::StringBuffer buffer;
      JsonWriter writer(buffer);
      it->second(this, &src, &writer);
      payload_t result = Utils::AllocatePayload(buffer.GetSize() + 1);
      result.length = buffer.GetSize();
      // TODO(Naoki): How can we avoid the memory copy?
      memcpy(result.data, buffer.GetString(), result.length + 1);
      return result;
    } else {
      return {
          .data = nullptr,
          .length = 0,
      };
    }
  }

  ObjectMapper();

 private:
  /**
   * Map of type index and JSON reader functions. Used for resolving the reader
   * function by type.
   */
  std::unordered_map<std::type_index,
                     std::function<void *(const ObjectMapper *, const rapidjson::Value *)>>
      readers;

  /**
   * Map of type index and JSON writer functions. Used for resolving the writer
   * function by type.
   */
  std::unordered_map<std::type_index,
                     std::function<void(const ObjectMapper *, const void *src, JsonWriter *writer)>>
      writers;

  template <typename T>
  void AddReader(std::function<void *(const ObjectMapper *, const rapidjson::Value *)> reader) {
    readers[std::type_index(typeid(T))] = reader;
  }

  // Deprecated -- Use the next version that uses JsonWriter for a new value
  // writer..
  template <typename T>
  void AddWriter(
      std::function<void(const T *, rapidjson::Value *, rapidjson::MemoryPoolAllocator<> &)>
          value_writer) {
    writers[std::type_index(typeid(T))] = [=](const ObjectMapper *, const void *src,
                                              JsonWriter *writer) {
      rapidjson::Document document;
      value_writer(reinterpret_cast<const T *>(src), &document, document.GetAllocator());
      document.Accept(*writer);
    };
  }

  template <typename T>
  void AddWriter(std::function<void(const ObjectMapper *, const T &, JsonWriter *)> value_writer) {
    writers[std::type_index(typeid(T))] = [=](const ObjectMapper *mapper, const void *src,
                                              JsonWriter *writer) {
      value_writer(mapper, *reinterpret_cast<const T *>(src), writer);
    };
  }

  static void FetchString(const rapidjson::Value *value, const char *name, std::string *target);

  static void FetchInt(const rapidjson::Value *value, const char *name, int32_t *target);

  static void FetchLong(const rapidjson::Value *value, const char *name, int64_t *target);

  static void PutString(rapidjson::Value *value, const char *name, const std::string &str,
                        rapidjson::MemoryPoolAllocator<> &allocator);

  static void PutString(const char *name, const std::string &str, JsonWriter *writer);

  static void PutInt(rapidjson::Value *value, const char *name, int32_t int_value,
                     rapidjson::MemoryPoolAllocator<> &allocator);

  static void PutInt(const char *name, int32_t int_value, JsonWriter *writer);

  static void PutLong(rapidjson::Value *value, const char *name, int64_t long_value,
                      rapidjson::MemoryPoolAllocator<> &allocator);

  static void PutLong(const char *name, int64_t long_value, JsonWriter *writer);

  static void PutBool(rapidjson::Value *value, const char *name, bool bool_value,
                      rapidjson::MemoryPoolAllocator<> &allocator);

  static void PutBool(const char *name, bool bool_value, JsonWriter *writer);

  template <typename T>
  static void PutAny(
      rapidjson::Value *value, const char *name, const T *src,
      std::function<void(const T *, rapidjson::Value *, rapidjson::MemoryPoolAllocator<> &)>
          value_writer,
      rapidjson::MemoryPoolAllocator<> &allocator);

  template <typename V>
  static std::function<void *(const ObjectMapper *, const rapidjson::Value *)> vector_reader;

  template <typename V>
  static std::function<void(const ObjectMapper *, const std::vector<V> &, JsonWriter *)>
      vector_writer;

  static std::function<void(const ObjectMapper *, const AdminOpType &, JsonWriter *)>
      admin_op_writer;

  static std::function<void(const ObjectMapper *, const RequestPhase &, JsonWriter *)>
      request_phase_writer;

  static std::function<void(const ObjectMapper *, const NodeType &, JsonWriter *)> node_type_writer;
};

}  // namespace tfos::csdk
#endif  // TFOS_COMMON_OBJECT_MAPPER_H_
