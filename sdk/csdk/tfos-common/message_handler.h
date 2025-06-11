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
#ifndef TFOS_CSDK_MESSAGE_HANDLER_H_
#define TFOS_CSDK_MESSAGE_HANDLER_H_

#include <string>
#include <vector>

#include "tfoscsdk/message_struct.h"

namespace tfos::csdk {

class ConnectionSet;
class ConnectionManager;
class SessionController;
class ObjectMapper;

/**
 * Abstract class to handle messages pushed by SessionController.
 *
 * The main purpose of this modules is message routing. The module has to be
 * aware of available endpoints. It also has to be aware which of available
 * endpoints are accessible (some endpoints may be down or "service unavailable"
 * for failure recovery).
 *
 * Routing tasks include:
 *   - Load balancing for a regular operation
 *   - Fan routing
 *   - Failure recovery procedure:
 *     - Send a failure report on a failure of the final phase of fan routing.
 *     - Avoid endpoints that are marked down
 *     - Resume access when a failed node has been recovered
 */
// TODO(Naoki): Consider consolidating this class into SessionController.
//              This class does not do many jobs and is simple enough to be
//              embedded into the SessionController. Consolidating saves a stage
//              of call chain.
class MessageHandler {
 public:
  virtual ~MessageHandler() = default;

  virtual void InjectSessionController(SessionController *session_controller) = 0;

  virtual void GetConnectionSet(const std::string &host, int port, bool ssl_enabled,
                                const std::string &cert_file,
                                void (*cb)(const ResponseMessage &resp, ConnectionSet *conn_set,
                                           void *cb_args),
                                void *cb_arg) = 0;
  /**
   * Method to handle a request message.
   *
   * The method call runs asynchronously. This method places the request to the
   * server and exists immediately. The backend invokes the response_cb when the
   * server returns the response.
   *
   * @param request_message The request message.
   */
  virtual void PlaceRequest(RequestMessage *request_message) = 0;

  virtual void ScheduleTask(void (*run)(void *), void *args, uint64_t millis) = 0;
  virtual void ScheduleTask(void (*run)(void *), void (*on_shutting_down)(void *), void *args,
                            uint64_t millis) {}

  virtual std::string name() = 0;
};

class MessageHandlerFactory {
 public:
  /**
   * Create a MessageHandler instance of an implementation specified by the
   name.
   *
   * @param implementation_name Name of the implementation.
                                Give an empty string ("") for the default
   implementation.
   * @return A HandleMessage instance or nullptr when the implementation name is
   unknown.
   */
  static MessageHandler *Create(const std::string &implementation_name,
                                const ObjectMapper *object_mapper, ConnectionManager *conn_manager);
};

}  // namespace tfos::csdk
#endif  // TFOS_CSDK_MESSAGE_HANDLER_H_
