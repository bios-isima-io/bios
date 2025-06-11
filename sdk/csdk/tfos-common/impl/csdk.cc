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
#include "tfoscsdk/csdk.h"

#include <stdio.h>
#include <string.h>

#include <iostream>
#include <string>

#include "tfos-common/utils.h"
#include "tfoscsdk/log.h"
#include "tfoscsdk/status.h"

using tfos::csdk::OpStatus;
using tfos::csdk::Statuses;
using tfos::csdk::Utils;

payload_t CsdkAllocatePayload(int capacity) {
  payload_t payload = Utils::AllocatePayload(capacity);
  DEBUG_LOG("Allocated payload data %p", payload.data);
  return payload;
}

void CsdkReleasePayload(payload_t *payload) {
  DEBUG_LOG("Releasing payload data %p", payload->data);
  Utils::ReleasePayload(payload);
}

string_t CsdkGetOperationName(CSdkOperationId id) { return Utils::GetOperationName(id); }

payload_t CsdkListStatusCodes() {
  payload_t out;
  uint32_t num_entries = static_cast<uint32_t>(OpStatus::END);
  uint32_t entry_size = sizeof(status_code_t);
  out.length = num_entries * entry_size;
  out.data = new uint8_t[out.length];
  for (uint32_t i = 0; i < num_entries; ++i) {
    OpStatus status = static_cast<OpStatus>(i);
    status_code_t status_code = Statuses::StatusCode(status);
    memcpy(&out.data[i * entry_size], &status_code, entry_size);
  }
  return out;
}

string_t CsdkGetStatusName(status_code_t code) {
  return Statuses::CName(Statuses::FromStatusCode(code));
}

int CsdkWriteHello(payload_t *payload) {
  std::string src = "hello world";
  size_t size =
      static_cast<size_t>(payload->length) < src.size() + 1 ? payload->length - 1 : src.size();
  DEBUG_LOG("writing %ld bytes to %p", size, payload->data);
  memcpy(payload->data, src.c_str(), size);
  payload->data[size] = 0;
  return size;
}
