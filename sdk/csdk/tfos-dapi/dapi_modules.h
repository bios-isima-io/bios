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
#ifndef TFOSCSDK_DAPI_MODULES_H_
#define TFOSCSDK_DAPI_MODULES_H_

#include <array>
#include <string>

#include "tfoscsdk/directapi.h"

namespace tfos::csdk {

class ConnectionManager;
class MessageHandler;
class ObjectMapper;
class SessionController;

// Creating 16 set of C-SDK modules won't happen in real world. The number
// should be one usually.
#define kMaxNumInstances 16

class DirectApiModules {
 private:
  static std::array<DirectApiModules *, kMaxNumInstances> instances;
  static capi_t next_instance_id;
  const ObjectMapper *object_mapper_;
  ConnectionManager *conn_manager_;
  SessionController *session_controller_;
  MessageHandler *message_handler_;

 public:
  /**
   * Create a CAPI module.
   *
   * @params modules_profile Module profile name
   * @return CAPI modules ID on successful creation, -1 on failure.
   */
  static capi_t Create(const std::string &modules_profile);

  /**
   * Converts a CAPI id to DirectApiModules object.
   *
   * @params capi CAPI modules ID
   * @params cb Callback function called when the conversion fails.
   *     when you do not need the result callback.
   * @params cb_args callback arguments
   * @return Pointer to the specified DirectApiModules or nullptr when no
   * modules is not found.
   */
  static DirectApiModules *Get(capi_t capi, capi_completion_cb cb, void *cb_args);

  /**
   * Converts a CAPI id to DirectApiModules object but without callback
   *
   * @params capi CAPI modules ID
   * @return Pointer to the specified DirectApiModules or nullptr when no
   * modules is not found.
   */
  inline static DirectApiModules *Get(capi_t capi) { return Get(capi, nullptr, nullptr); }

  /**
   * Terminates a CAPI module
   *
   * @params CAPI modules ID
   */
  static void Terminate(capi_t capi);

  // Getters
  inline const ObjectMapper *object_mapper() const { return object_mapper_; }
  inline ConnectionManager *conn_manager() const { return conn_manager_; }
  inline SessionController *session_controller() const { return session_controller_; }
  inline MessageHandler *message_handler() const { return message_handler_; }

 private:
  static bool Verify(DirectApiModules *modules, capi_completion_cb cb, void *cb_args);
  DirectApiModules(const std::string &modules_profile);
  virtual ~DirectApiModules();
};

}  // namespace tfos::csdk
#endif  // TFOSCSDK_DAPI_MODULES_H_
