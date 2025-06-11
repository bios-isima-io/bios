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
#include "tfos-dapi/dapi_modules.h"

#include <iostream>
#include <string>
#include <type_traits>

#include "tfos-common/logger.h"
#include "tfos-common/message_handler.h"
#include "tfos-common/session.h"
#include "tfos-physical/connection_manager.h"
#include "tfos-physical/impl/physical_pipe_impl.h"

namespace tfos::csdk {

std::array<DirectApiModules *, kMaxNumInstances> DirectApiModules::instances;
capi_t DirectApiModules::next_instance_id = 0;

// WARNING: Thread unsafe
capi_t DirectApiModules::Create(const std::string &modules_profile) {
  // find an available slot
  capi_t id = next_instance_id;
  if (id == kMaxNumInstances) {
    for (id = 0; id < next_instance_id; ++id) {
      if (instances[id] == nullptr) {
        break;
      }
    }
  }
  if (id >= kMaxNumInstances) {
    // no vacancy
    return -1;
  }

  // create an instance
  auto new_instance = new DirectApiModules(modules_profile);
  if (!Verify(new_instance, nullptr, nullptr)) {
    delete new_instance;
    return -1;
  }

  // put the new instance into the free slot
  instances[id] = new_instance;
  next_instance_id = (id + 1) % kMaxNumInstances;
  int count = 0;
  while (count < kMaxNumInstances && instances[next_instance_id] != nullptr) {
    ++count;
    next_instance_id = (next_instance_id + 1) % kMaxNumInstances;
  }
  if (count == kMaxNumInstances) {
    next_instance_id = kMaxNumInstances;
  }
  return id;
}

DirectApiModules *DirectApiModules::Get(capi_t capi, capi_completion_cb cb, void *cb_args) {
  auto *modules = (capi >= 0 && capi < kMaxNumInstances) ? instances[capi] : nullptr;
  Verify(modules, cb, cb_args);
  return modules;
}

bool DirectApiModules::Verify(DirectApiModules *modules, capi_completion_cb cb, void *cb_args) {
  OpStatus status = OpStatus::OK;
  payload_t payload;
  if (modules == nullptr) {
    payload = Utils::MakePayload("Null API object");
    status = OpStatus::GENERIC_CLIENT_ERROR;
  } else if (modules->message_handler() == nullptr) {
    payload = Utils::MakePayload("Message Handler has not been intialized");
    status = OpStatus::GENERIC_CLIENT_ERROR;
  }
  if (status != OpStatus::OK && cb != nullptr) {
    cb(Statuses::StatusCode(status), {0}, payload, cb_args);
  }
  return status == OpStatus::OK;
}

void DirectApiModules::Terminate(capi_t capi) {
  auto to_delete = (capi >= 0 && capi < kMaxNumInstances) ? instances[capi] : nullptr;
  if (to_delete != nullptr) {
    instances[capi] = nullptr;
  }
  delete to_delete;
}

DirectApiModules::DirectApiModules(const std::string &modules_profile) {
  // TODO(Naoki): Hide this behind the ConnectionManager.
  PhysicalPipeImpl::GlobalInit();
  object_mapper_ = new ObjectMapper();
  conn_manager_ = ConnectionManagerFactory::Create(modules_profile, object_mapper_);
  message_handler_ = MessageHandlerFactory::Create(modules_profile, object_mapper_, conn_manager_);
  if (message_handler_ == nullptr) {
    DEBUG_LOG("ERROR: MessageHandler was not created -- invalid profile: %s",
              modules_profile.c_str());
  }
  session_controller_ = new SessionController(object_mapper_, message_handler_);
  if (message_handler_ != nullptr) {
    message_handler_->InjectSessionController(session_controller_);
  }
}

DirectApiModules::~DirectApiModules() {
  delete message_handler_;
  delete session_controller_;
  delete conn_manager_;
  delete object_mapper_;
}

}  // namespace tfos::csdk
