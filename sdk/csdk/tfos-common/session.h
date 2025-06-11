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
#ifndef TFOSCSDK_SESSION_H_
#define TFOSCSDK_SESSION_H_

#include <atomic>
#include <cstdint>
#include <map>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "tfos-common/object_mapper.h"
#include "tfoscsdk/csdk.h"
#include "tfoscsdk/message_struct.h"
#include "tfoscsdk/models.h"
#include "tfoscsdk/types.h"

namespace tfos::csdk {

class Connection;
class Logger;
class MessageHandler;
class MethodCallState;
class SessionController;
class Session;

class SessionController final {
 private:
  // session controller metadata
  std::atomic_int32_t next_session_id_;
  // TODO(Naoki): We may want concurrent hash map.
  std::unordered_map<int, Session *> sessions_;
  std::unordered_set<Session *> stale_sessions_;
  // ID of a closed session is stored here. Entries never get deleted, assuming there won't be
  // too many closed sessions in a process.
  std::unordered_set<int32_t> invalidated_sessions_;
  mutable std::shared_mutex sessions_mutex_;

  // Dependency components
  const ObjectMapper *const object_mapper_;
  MessageHandler *const message_handler_;

 public:
  SessionController(const ObjectMapper *object_mapper, MessageHandler *message_handler);
  ~SessionController();

  void StartSession(const std::string &host, int port, bool ssl_enabled,
                    const std::string &journal_dir, const std::string &cert_file,
                    capi_completion_cb api_cb, void *api_cb_args, int64_t timeout);

  bool SetOpTimeout(int32_t session_id, int64_t timeout);

  void CloseSession(int32_t session_id);

  /**
   * This method executes signing into the server with target scope.
   *
   * The payload should include credentials to sign in.
   * The callback function would receive a serialized NativeLoginResponse.
   * object instead of the server response from the login operation.
   * See tfos-common/models.h for the definition of NativeLoginResponse.
   */
  void Login(int32_t session_id, payload_t payload, bool disable_routing, capi_completion_cb cb,
             void *cb_args);

  void AddEndpoint(int32_t session_id, const std::string &endpoint, CSdkNodeType node_type,
                   capi_completion_cb cb, void *cb_args);

  void SimpleMethod(int32_t session_id, CSdkOperationId op_id, const std::string *tenant_name,
                    const std::string *stream_name, payload_t payload, const std::string *endpoint,
                    payload_t *metric_timestamps, capi_completion_cb cb, void *cb_args,
                    bool is_metrics_logging = false);

  void GenericMethod(int32_t session_id, CSdkOperationId op_id, string_t resources[],
                     string_t options[], payload_t payload, int64_t timeout,
                     const std::string *endpoint, payload_t *metric_timestamps,
                     capi_completion_cb cb, void *cb_args);

  void IngestBulkStart(int32_t session_id, CSdkOperationId op_id, int num_events,
                       capi_completion_cb api_cb, void (*ask_cb)(int64_t, int, int, void *),
                       void *cb_args);

  void IngestBulk(int32_t session_id, int64_t ingest_bulk_ctx_id, int from_index, int to_index,
                  string_t resources[], string_t options[], payload_t payload,
                  payload_t *metric_timestamps, capi_completion_cb cb, void *cb_args);

  void IngestBulkEnd(int32_t session_id, int64_t ingest_bulk_ctx_id);

  /**
   * Checks whether the specified specified session has permission to run the
   * specified op.
   *
   * The method executes the callback function with an error status only when
   * the permission is denied.
   *
   * @param session_id The session ID
   * @param op_id The operation ID
   * @param cb The error callback function; Specifying nullptr omits the
   * callback.
   * @param cb_args The error callback function application data
   * @return true if the operation is permitted, false otherwise
   */
  bool CheckOperationPermission(int32_t session_id, CSdkOperationId op_id, capi_completion_cb cb,
                                void *cb_args);

  void FlushClientMetrics(void);

  Session *GetSession(int32_t session_id);

  /**
   * Tests whether a session is valid.
   *
   * When a session is closed, there may be some request in the session that is pending for
   * retry. There is a small possibility that the request would come back after the session is
   * destroyed. In order to avoid this issue, the session controller remembers invalidated session
   * IDs upon their close requests. The method looks up those closed sessions and returns
   * if the session is still valid.
   *
   * NOTE: This method uses a global lock internally, do not call the method from a
   *       performance-critical code path.
   */
  bool CheckSessionValidity(int32_t session_id) const;

  // constants
  // /////////////////////////////////////////////////////////////////////////////////
  /**
   * Scope string for super admin login
   */
  static const char *SUPER_ADMIN_SCOPE;
  /**
   * Lead time in milliseconds to send session renewal request before expiry
   */
  static const int64_t MIN_RENEW_SESSION_LEAD_TIME;
  /**
   * Duration in milliseconds to back-off session renewal since one operator
   * sends a renewal. If the renewal was not successful within this time,
   * another operation would retry.
   */
  static const int64_t RENEWAL_BLACKOUT_MILLIS;

 private:
  bool AddSession(int32_t session_id, Session *session);
  bool RemoveSession(int32_t session_id);
  Session *FindSession(int32_t session_id, capi_completion_cb api_cb, void *api_cb_args);
  int32_t GetNextSessionId();

  /**
   * Utility method to check whether the session allows the operation.
   *
   * If the operation is not allowed, the method returns FORBIDDEN error via the
   * API callback.
   *
   * @param session The session
   * @param op_id The operation ID
   * @param api_cb API callback function
   * @param api_cb_args Arguments for the API callback function
   * @return true if the operation is allowed, false otherwise.
   */
  bool IsOperationAllowed(Session *session, CSdkOperationId op_id, capi_completion_cb api_cb,
                          void *api_cb_args);

  void CheckSessionExpiry(Session *session);

  /**
   * Callback function to handle GetConnectionSet request completion against
   * message handler.
   */
  static void CompleteGetConnectionSet(const ResponseMessage &response, ConnectionSet *conn_set,
                                       void *cb_args);

  /**
   * Callback function to handle Login operation response.
   */
  static void CompleteLoginBIOS(const ResponseMessage &response, void *cb_args);
  // subtasks of CompleteLoginBIOS()
  static bool CheckLoginResponse(const ResponseMessage &response, MethodCallState *state, bool v2);
  static void HandleSuccessfulLogin(const ResponseMessage &response, MethodCallState *state);
  static void HandleSuccessfulGetEndpointsPostLogin(const ResponseMessage &response,
                                                    MethodCallState *state, bool is_final = true,
                                                    bool to_respond = true);
  static void CompleteUpstreamRegistration(const ResponseMessage &response,
                                           UpstreamConfig *upstream_config, void *cb_args);
  static void CompleteConnRegistration(const ResponseMessage &response, Connection *conn,
                                       void *cb_args);

  static void CompleteGetEndpointsPostLogin(const ResponseMessage &response, void *cb_args);
  static void CompleteGetUpstreamPostLogin(const ResponseMessage &response, void *cb_args);

  /**
   * Callback function to handle GetStream operation after successful non-admin
   * login.
   */
  static void CompleteGetStreamPostLogin(const ResponseMessage &response, void *cb_args);

  /**
   * Callback function to complete a session renewal.
   */
  static void CompleteRenewSession(const ResponseMessage &response, void *state);

  /**
   * callback function to handle a simple method response.
   */
  static void CompleteOperationGeneric(const ResponseMessage &response, void *cb_args);

  /**
   * Callback function to complete an IngestBulk operation.
   */
  static void CompleteIngestBulk(status_code_t status_code, completion_data_t *completion_data,
                                 payload_t response_data, void *cb_args);

  /**
   * Generic method to send a request to the server.
   *
   * The method call runs asynchronously. This method places the request to the
   * server and exists immediately. The backend invokes the completion_cb when
   * the server returns the response.
   *
   * Internally, this method builds a RequestMessage that includes all necessary
   * information to conduct a server method call. So any new operation must
   * start from this method to reach the next layer although a chain call may
   * call message_handler_->PlaceRequest() directory (since the chain call may
   * not be a NEW one).
   *
   * @param op_id Operation ID
   * @param tenant_name Tenant name. Put nullptr not to specify.
   * @param stream_name Stream name. Put nullptr not to specify.
   * @param payload Request data payload.
   * @param session The session where this method call occurs.
   * @param endpoint
   *        Endpoint URL to specify the target server explicitly. The SDK
   * follows its internal routing rule when nullptr is specified. The instance
   * of the endpoint is owned by caller and the object is referred only while
   * this method is executed. The caller must release memory after the method
   * call at the earliest necessary timing.
   * @param metric_timestamps Buffer to put timestamps for metrics. Put nullptr
   * to disable metrics.
   * @param completion_cb
   *        Callback function that is invoked when the server sends back the
   * response. Since the function would be called after this method completes.
   * @param completion_cb_args
   *         Arguments brought by the caller for completion_cb. All resources
   * must be available until the completion_cb consumes the data. Also, assume
   * that the thread that runs the callback function is different from one that
   * runs this method.
   * @param options Options
   *
   * @type T Type of completion_cb_args. The type has a restriction that must
   * have member RequestMessage *request_message; to safely invoke
   * ClenupCommonCallParameters() below in the callback function.
   */
  void SendRequest(CSdkOperationId op_id, const std::string *tenant_name,
                   const std::string *stream_name, const payload_t &payload, Session *session,
                   const std::string *endpoint, payload_t *metric_timestamps,
                   response_cb completion_cb, MethodCallState *completion_cb_args,
                   std::map<std::string, std::string> *options = nullptr,
                   std::map<std::string, std::string> *path_params = nullptr,
                   bool is_metrics_logging = false);

  /**
   * The method cleans up the request message fromt the MethodCallState.
   */
  static void CleanupRequestMessage(MethodCallState *state);

  static void CompleteLogging(status_code_t status_code, completion_data_t *completion_data,
                              payload_t response_data, void *cb_args);

  static void FlushClientMetricsTask(void *cb_args);
  static void FlushClientMetricsFinalTask(void *cb_args);
};

}  // namespace tfos::csdk
#endif  // TFOSCSDK_SESSION_CONTROLLER_H_
