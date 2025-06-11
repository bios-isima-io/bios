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
#include "tfos-common/message_handler.h"

#include <assert.h>
#include <string.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "tfos-common/event_loop.h"
#include "tfos-common/impl/mock_message_handler.h"
#include "tfos-common/keywords.h"
#include "tfos-common/object_mapper.h"
#include "tfos-common/session.h"
#include "tfos-physical/connection.h"
#include "tfos-physical/connection_manager.h"
#include "tfos-physical/physical_pipe.h"
#include "tfoscsdk/log.h"

namespace tfos::csdk {

class BasicMessageHandler final : public MessageHandler {
 public:
  class RegularOpState;

 private:
  const ObjectMapper *const object_mapper_;
  ConnectionManager *const conn_manager_;
  const SessionController *session_controller_;

  std::vector<ConnectionSet *> conn_sets_;
  mutable std::shared_mutex conn_sets_mutex_;

  EventLoop *event_loop_;
  std::mutex *shutdown_mutex_;
  std::condition_variable *shutdown_cv_;
  volatile int shutdown_state_;  // 0: not yet, 1: shutting down, 2: completed

 public:
  std::string name() { return "basic"; }
  EventLoop *event_loop() const { return event_loop_; }

  BasicMessageHandler(const ObjectMapper *object_mapper, ConnectionManager *mgr)
      : object_mapper_(object_mapper),
        conn_manager_(mgr),
        session_controller_(nullptr),
        event_loop_(new EventLoop()),
        shutdown_mutex_(nullptr),
        shutdown_cv_(nullptr),
        shutdown_state_(0) {}

  ~BasicMessageHandler() {
    shutdown_mutex_ = new std::mutex();
    shutdown_cv_ = new std::condition_variable();

    if (event_loop_->IsValid()) {
      event_loop_->PutFinalTask(TerminateHandler, this);
      while (shutdown_state_ < 2) {
        std::unique_lock<std::mutex> lock(*shutdown_mutex_);
        shutdown_cv_->wait_for(lock, std::chrono::milliseconds(100));
      }
      event_loop_->Shutdown();
    } else {
      delete event_loop_;
    }

    delete shutdown_mutex_;
    delete shutdown_cv_;

    for (ConnectionSet *conn_set : conn_sets_) {
      delete conn_set;
    }
  }

  void InjectSessionController(SessionController *session_controller) {
    session_controller_ = session_controller;
  }

  static void TerminateHandler(void *arg) {
    auto handler = reinterpret_cast<BasicMessageHandler *>(arg);

    handler->shutdown_state_ = 2;
    handler->shutdown_cv_->notify_all();
  }

  struct GetConnectionSetState {
    void (*caller_cb)(const ResponseMessage &resp, ConnectionSet *conn_set, void *cb_args);
    void *caller_cb_args;
    BasicMessageHandler *handler;
    GetConnectionSetState(void (*cb)(const ResponseMessage &resp, ConnectionSet *conn_set,
                                     void *cb_args),
                          void *cb_args, BasicMessageHandler *h)
        : caller_cb(cb), caller_cb_args(cb_args), handler(h) {}
  };

  static void CreateConnectionSetCb(const ResponseMessage &response_message,
                                    ConnectionSet *conn_set, void *cb_args) {
    auto *state = reinterpret_cast<GetConnectionSetState *>(cb_args);
    state->handler->AddConnectionSet(conn_set);
    state->caller_cb(response_message, conn_set, state->caller_cb_args);
    delete state;
  }

  void GetConnectionSet(const std::string &host, int port, bool ssl_enabled,
                        const std::string &ssl_cert_file,
                        void (*cb)(const ResponseMessage &resp, ConnectionSet *conn_set,
                                   void *cb_args),
                        void *cb_args) {
    if (!ssl_enabled) {
      ErrorResponse response("Insecure connection is not supported");
      payload_t payload = object_mapper_->WriteValue(response);
      ResponseMessage response_message(OpStatus::SERVER_CONNECTION_FAILURE, "", payload.data,
                                       payload.length);
      cb(response_message, nullptr, cb_args);
      return;
    }
    std::string endpoint = Connection::MakeEndpointString(host, port, ssl_enabled);
    ConnectionSet *conn_set = FindConnectionSet(endpoint);
    if (conn_set != nullptr) {
      ResponseMessage response_message(OpStatus::OK, endpoint, nullptr, 0);
      cb(response_message, conn_set, cb_args);
      return;
    }
    auto *state = new GetConnectionSetState(cb, cb_args, this);
    conn_manager_->CreateConnectionSet(host, port, ssl_enabled, ssl_cert_file,
                                       CreateConnectionSetCb, state);
  }

  void PlaceRequest(RequestMessage *request_message) {
    // sanity checks
    assert(request_message != nullptr);
    RequestContext *context = request_message->context();
    assert(context != nullptr);
    assert(context->session != nullptr);
    assert(context->session->connection_set != nullptr);
    assert(context->completion_cb != nullptr);
    assert(context->completion_cb_args != nullptr);
    (void)context;

    RegularOpState *regular_op_state = new RegularOpState(this, request_message);
    if (!AssignUpstreams(regular_op_state)) {
      return;
    }
    regular_op_state->pending_callbacks.fetch_add(1);
    HandleRequestGeneric(regular_op_state);
  }

  void ScheduleTask(void (*run)(void *), void *args, uint64_t millis) {
    event_loop_->ScheduleTask(run, args, millis);
  }

  void ScheduleTask(void (*run)(void *), void (*on_shutting_down)(void *), void *args,
                    uint64_t millis) {
    event_loop_->ScheduleTask(run, on_shutting_down, args, millis);
  }

  class RegularOpState {
   public:
    BasicMessageHandler *const handler;
    RequestMessage *const original_request;
    RequestMessage *internal_request;
    RequestMessage *qos_request;
    std::atomic<int> pending_callbacks;
    std::atomic<int> response_sent;

   private:
    // Carries available upstreams
    std::vector<Connection *> upstreams_;
    std::vector<Connection *> backup_upstreams_;
    volatile uint32_t upstream_index_;

    Connection *current_conn_;
    Connection *qos_conn_;
    OpStatus last_status_;
    payload_t last_response_message_;

    int retry_count_;
    uint64_t retry_delay_ms_;
    int32_t lookup_qos_threshold_millis_;
    bool qos_retry_considered_;
    bool qos_retry_sent_;
    bool qos_retry_response_used_;

   public:
    RegularOpState(BasicMessageHandler *handler, RequestMessage *original_request)
        : handler(handler),
          original_request(original_request),
          internal_request(nullptr),
          qos_request(nullptr),
          pending_callbacks(0),
          response_sent(0),
          upstream_index_(0),
          current_conn_(nullptr),
          qos_conn_(nullptr),
          last_status_(OpStatus::END),
          last_response_message_{0},
          retry_count_(0),
          retry_delay_ms_(10),
          lookup_qos_threshold_millis_(0),
          qos_retry_considered_(false),
          qos_retry_sent_(false),
          qos_retry_response_used_(false) {
      assert(handler != nullptr);
      assert(original_request != nullptr);
      assert(original_request->context() != nullptr);
      assert(original_request->context()->session != nullptr);
      if ((original_request->context() != nullptr) &&
          (original_request->context()->session != nullptr)) {
        lookup_qos_threshold_millis_ =
            original_request->context()->session->GetLookupQosThresholdMillis();
      }
    }

    ~RegularOpState() {
      if (last_response_message_.length > 0) {
        Utils::ReleasePayload(&last_response_message_);
      }
      ClearInternalRequest();
      ClearQosRequest();
    }

    inline std::vector<Connection *> &upstreams() { return upstreams_; }
    inline std::vector<Connection *> &backup_upstreams() { return backup_upstreams_; }
    inline OpStatus last_status() const { return last_status_; }
    inline int retry_count() const { return retry_count_; }
    inline uint64_t retry_delay_ms() const { return retry_delay_ms_; }

    /*
     * Returns Lookup Qos Threshold in miliseconds. 0 if the feature is disabled.
     */
    inline int32_t GetLookupQosThresholdMillis() const {
      if (((original_request->op_id() == CSdkOperationId::CSDK_OP_MULTI_GET_CONTEXT_ENTRIES_BIOS) ||
           (original_request->op_id() == CSdkOperationId::CSDK_OP_GET_CONTEXT_ENTRIES_BIOS)) &&
          (original_request->context()->session->is_realtime)) {
        return lookup_qos_threshold_millis_;
      }

      return 0;
    }

    inline void set_qos_retry_considered(bool value) { qos_retry_considered_ = value; }

    inline void set_qos_retry_sent(bool value) { qos_retry_sent_ = value; }

    inline void set_qos_retry_response_used(bool value) { qos_retry_response_used_ = value; }

    payload_t RetrieveLastResponse() {
      payload_t reply = last_response_message_;
      last_response_message_.data = nullptr;
      last_response_message_.length = 0;
      return reply;
    }

    void ClearInternalRequest() {
      if (internal_request) {
        delete internal_request->context();
      }
      delete internal_request;
      internal_request = nullptr;
    }

    void ClearQosRequest() {
      if (qos_request) {
        delete qos_request->context();
      }
      delete qos_request;
      qos_request = nullptr;
    }

    Connection *GetNextConnection() {
      DEBUG_LOG("GetNextConnection index=%d upstreams=%ld", upstream_index_, upstreams_.size());
      if (upstream_index_ < static_cast<int64_t>(upstreams_.size())) {
        current_conn_ = upstreams_.at(upstream_index_++);
        if (current_conn_ == qos_conn_) {
          if (upstream_index_ < static_cast<int64_t>(upstreams_.size())) {
            current_conn_ = upstreams_.at(upstream_index_++);
            return current_conn_;
          } else {
            return nullptr;
          }
        }
        return current_conn_;
      } else {
        return nullptr;
      }
    }

    Connection *GetNextConnectionForQos() {
      DEBUG_LOG("GetNextConnectionForQos index=%d upstreams=%lu backup_upstreams=%lu",
                upstream_index_, upstreams_.size(), backup_upstreams_.size());

      if ((upstreams_.size() == 1) && (backup_upstreams_.size() > 0)) {
        // get backup connection if there is only 1 active upstream
        Connection *conn = backup_upstreams_.at(0);
        qos_conn_ = conn;
        return conn;
      } else if ((upstreams_.size() == 1) && (backup_upstreams_.size() == 0)) {
        // only 1 connection
        DEBUG_LOG("Cannot run Qos task as there is only 1 upstream endpoint");
        return nullptr;
      }

      // more than 1 active connections, get the one which is not used currently
      Connection *conn = upstreams_.at(upstream_index_ % upstreams_.size());
      upstream_index_++;
      if (conn == current_conn_) {
        conn = upstreams_.at(upstream_index_ % upstreams_.size());
        upstream_index_++;
      }
      qos_conn_ = conn;
      return qos_conn_;
    }

    void PrepareRetry() {
      upstream_index_ = 0;
      ++retry_count_;
      retry_delay_ms_ *= 2;
    }

    Connection *GetCurrentConnection() { return current_conn_; }
    Connection *GetQosConnection() { return qos_conn_; }

    void SetLastResult(const ResponseMessage &response) {
      last_status_ = response.status();
      if (last_response_message_.length > 0) {
        Utils::ReleasePayload(&last_response_message_);
      }
      last_response_message_ = response.payload();
    }

    void Complete(const ResponseMessage &response) {
      int expected = 0;
      if (response_sent.compare_exchange_strong(expected, 1)) {
        ResponseMessage new_response(response.status(), response.endpoint(), response.payload());
        new_response.set_qos_retry_considered(qos_retry_considered_);
        new_response.set_qos_retry_sent(qos_retry_sent_);
        new_response.set_qos_retry_response_used(qos_retry_response_used_);
        original_request->context()->completion_cb(new_response,
                                                   original_request->context()->completion_cb_args);
      }
    }
  };

  /**
   * Method to assign upstream connections to list of connections
   * state->upstreams.
   *
   * @param state Operation state
   * @return True if the caller should continue processing the operation.
   *    When this method makes a separate asynchronous call to continue the
   * operation, the method returns false. The caller should terminate
   * immediately in the case.
   */
  bool AssignUpstreams(RegularOpState *state) {
    auto context = state->original_request->context();
    if (context->endpoint != nullptr) {
      if (state->original_request->op_id() == CSDK_OP_UPDATE_ENDPOINTS) {
        // We need special endpoint request handling for AddEndpoint operation.
        // The requested endpoint URL may not be in the connection set yet since
        // C-SDK load balancer may not have received that unregistered endpoint
        // yet.
        return AssignUpstreamForAddEndpoint(state);
      } else {
        auto conn = context->session->connection_set->GetConnection(
            Connection::MakeEndpointString(*context->endpoint));
        if (conn != nullptr) {
          state->upstreams().push_back(conn);
        } else {
          ErrorResponse response("No such endpoint: " + *context->endpoint);
          payload_t payload = object_mapper_->WriteValue(response);
          ResponseMessage response_message(OpStatus::SERVER_CONNECTION_FAILURE, "", payload);
          state->Complete(response_message);
          int32_t pending_callbacks = state->pending_callbacks.fetch_sub(1) - 1;
          assert(pending_callbacks >= 0);
          if (pending_callbacks <= 0) {
            delete state;
          }
          return false;
        }
      }
    } else {
      context->session->connection_set->GetConnectionsForOp(state->original_request->op_id(),
                                                            &state->upstreams(),
                                                            &state->backup_upstreams());
    }
    return true;
  }

  /**
   * This method runs special treatment of connection acquisition request for
   * AddEndpoint operation.
   *
   * AddEndpoint call must be sent to the endpoint specified by the request,
   * because the server will generate a endpoint to node ID (which is self)
   * mapping when handling the request. The node that handles the request must
   * be the one that owns the endpoint. But C-SDK may not have the connection
   * against the URL since it hasn't appear in the ListEndpoint() operation
   * result (AddEndpoint registers the endpoint). Because of this issue, The
   * C-SDK has to make a new connection for the endpoint if it's missing in the
   * ConnectionSet. This method takes care of it.
   *
   * Connection request result would be sent to the callback CompleteGetOne
   * where the operation against the endpoint is invoked on successful
   * connection.
   */
  bool AssignUpstreamForAddEndpoint(RegularOpState *state) {
    auto context = state->original_request->context();
    // TODO(Naoki): Parameterize the key
    auto it = context->resources.options.find(CSDK_KEY_NODE_TYPE);
    if (it == context->resources.options.end() ||
        it->second == std::to_string(CSDK_NODE_TYPE_SIGNAL)) {
      context->session->connection_set->GetOneSignal(*context->endpoint, CompleteGetOne, state);
    } else {
      context->session->connection_set->GetOneAdmin(*context->endpoint, CompleteGetOne, state);
    }
    // Another asynchrnous call would be made via CompleteGetOne. Returning
    // false.
    return false;
  }

  /**
   * Callback function invoked when the connection request from
   * AssignUpstreamForAddEndpoint completes. On successful connection, this
   * callback runs operation by HandleRequestGeneric. Otherwise the method
   * replies to the caller and finish.
   */
  static void CompleteGetOne(const ResponseMessage &response, Connection *conn, void *args) {
    auto state = reinterpret_cast<RegularOpState *>(args);
    if (response.status() == OpStatus::OK) {
      state->upstreams().push_back(conn);
      state->pending_callbacks.fetch_add(1);
      HandleRequestGeneric(state);
    } else {
      state->Complete(response);
      int32_t pending_callbacks = state->pending_callbacks.fetch_sub(1) - 1;
      assert(pending_callbacks >= 0);
      if (pending_callbacks <= 0) {
        delete state;
      }
    }
  }

  static void RetryGenericRequestTask(void *args) {
    DEBUG_LOG("BasicMessageHandler::RetryGenericRequest() Enter");
    auto state = reinterpret_cast<RegularOpState *>(args);
    DEBUG_LOG("RETRYING %ld", Utils::CurrentTimeMillis());
    auto handler = state->handler;
    auto session_id = state->original_request->context()->session_id;
    bool session_validity = handler->session_controller_->CheckSessionValidity(session_id);
    DEBUG_LOG("Session validity: %d", session_validity);
    if (handler->shutdown_state_ > 0 || !session_validity) {
      // The caller is not interested in the result anymore, cancelling.
      delete state;
      return;
    }
    // We don't use the previous result of this method. The upcoming retry result would be
    // used instead.
    state->handler->AssignUpstreams(state);
    state->PrepareRetry();
    HandleRequestGeneric(state);
    DEBUG_LOG("BasicMessageHandler::RetryGenericRequest() End");
  }

  static void HandleQosRequestGenericTask(void *args) {
    auto regular_op_state = reinterpret_cast<RegularOpState *>(args);
    if (regular_op_state->response_sent.load() == 1) {
      DEBUG_LOG("Response already sent, no need to schedule Qos task");
      int32_t pending_callbacks = regular_op_state->pending_callbacks.fetch_sub(1) - 1;
      if (pending_callbacks <= 0) {
        delete regular_op_state;
      }
      return;
    }
    auto conn = regular_op_state->GetNextConnectionForQos();
    if (conn == nullptr) {
      DEBUG_LOG("Cannot schedule QoS Task as no more servers are available");
      int32_t pending_callbacks = regular_op_state->pending_callbacks.fetch_sub(1) - 1;
      if (pending_callbacks <= 0) {
        delete regular_op_state;
      }
      return;
    }
    regular_op_state->set_qos_retry_sent(true);
    DEBUG_LOG("QOS OP; op=%s, endpoint=%s",
              Utils::GetOperationName(regular_op_state->original_request->op_id()).text,
              conn->endpoint().c_str());
    auto *context_orig = regular_op_state->original_request->context();
    auto *context_qos =
        new RequestContext(context_orig->session, context_orig->timeout, context_orig->endpoint,
                           CompleteQosRequestGeneric, regular_op_state,
                           context_orig->metric_timestamps, context_orig->is_metrics_logging);
    context_qos->resources = context_orig->resources;
    regular_op_state->qos_request = new RequestMessage({
        .op_id_ = regular_op_state->original_request->op_id(),
        .payload_ = regular_op_state->original_request->payload(),
        .context_ = context_qos,
    });
    conn->pipe()->HandleRequest(*regular_op_state->qos_request);
  }

  static void HandleRequestGeneric(RegularOpState *regular_op_state) {
    auto *conn = regular_op_state->GetNextConnection();
    if (conn == nullptr) {
      int retry_count = regular_op_state->retry_count();
      uint64_t delay = regular_op_state->retry_delay_ms();
      if (delay <= Connection::GetDefaultErrorExpiry() && retry_count < 16) {
        DEBUG_LOG("WOULD RETRY %ld credit=%d delay=%ld", Utils::CurrentTimeMillis(), retry_count,
                  delay);
        regular_op_state->handler->event_loop()->ScheduleTask(RetryGenericRequestTask,
                                                              regular_op_state, delay);
        return;
      }

      // No more connections available
      auto errors =
          regular_op_state->original_request->context()->session->connection_set->CollectErrors();
      DEBUG_LOG("REGULAR OP; no connections are available op=%s errors=%s",
                Utils::GetOperationName(regular_op_state->original_request->op_id()).text,
                errors.c_str());
      if (regular_op_state->last_status() == OpStatus::END) {
        ErrorResponse response("No endpoints are available; " + errors);
        payload_t payload = regular_op_state->handler->object_mapper_->WriteValue(response);
        ResponseMessage response_message(OpStatus::SERVER_CONNECTION_FAILURE, "", payload);
        regular_op_state->Complete(response_message);
      } else {
        ResponseMessage response_message(regular_op_state->last_status(), "",
                                         regular_op_state->RetrieveLastResponse());
        regular_op_state->Complete(response_message);
      }

      auto pending_callbacks = regular_op_state->pending_callbacks.fetch_sub(1) - 1;
      assert(pending_callbacks >= 0);
      if (pending_callbacks == 0) {
        delete regular_op_state;
      }
      return;
    }
    DEBUG_LOG("REGULAR OP; op=%s, endpoint=%s",
              Utils::GetOperationName(regular_op_state->original_request->op_id()).text,
              conn->endpoint().c_str());
    auto *context_orig = regular_op_state->original_request->context();
    auto *context_internal =
        new RequestContext(context_orig->session, context_orig->timeout, context_orig->endpoint,
                           CompleteRequestGeneric, regular_op_state,
                           context_orig->metric_timestamps, context_orig->is_metrics_logging);
    context_internal->resources = context_orig->resources;
    regular_op_state->internal_request = new RequestMessage({
        .op_id_ = regular_op_state->original_request->op_id(),
        .payload_ = regular_op_state->original_request->payload(),
        .context_ = context_internal,
    });

    // schedule a lookupQoS timer callback first as callback_cb can be invoked synchonously which
    // can lead to deleting op state before this function completes.
    if (regular_op_state->GetLookupQosThresholdMillis() > 0) {
      if (regular_op_state->GetQosConnection() == nullptr) {
        DEBUG_LOG("Scheduling Qos threshold timeout task; op=%s",
                  Utils::GetOperationName(regular_op_state->original_request->op_id()).text);
        regular_op_state->set_qos_retry_considered(true);
        regular_op_state->pending_callbacks.fetch_add(1);
        regular_op_state->handler->event_loop()->ScheduleTask(
            HandleQosRequestGenericTask, regular_op_state,
            regular_op_state->GetLookupQosThresholdMillis());
      }
    }

    conn->pipe()->HandleRequest(*regular_op_state->internal_request);
  }

  static void CompleteQosRequestGeneric(const ResponseMessage &response, void *cb_args) {
    DEBUG_LOG("Received response from Qos connection=%s, status=%s", response.endpoint().c_str(),
              Statuses::Name(response.status()).c_str());

    auto *regular_op_state = reinterpret_cast<RegularOpState *>(cb_args);
    int32_t pending_callbacks = regular_op_state->pending_callbacks.fetch_sub(1) - 1;
    assert(pending_callbacks >= 0);

    if (Utils::IsRetriableFailure(response.status()) && (pending_callbacks > 0)) {
      DEBUG_LOG("Recoverable error -- letting generic completion do the retry");
      regular_op_state->ClearQosRequest();
      regular_op_state->SetLastResult(response);
      return;
    }

    regular_op_state->set_qos_retry_response_used(true);
    regular_op_state->Complete(response);

    if (pending_callbacks <= 0) {
      delete regular_op_state;
    }
  }

  static void CompleteRequestGeneric(const ResponseMessage &response, void *cb_args) {
    DEBUG_LOG("Received response from current connection=%s, status=%s",
              response.endpoint().c_str(), Statuses::Name(response.status()).c_str());

    auto *regular_op_state = reinterpret_cast<RegularOpState *>(cb_args);

    bool is_retriable_failure = Utils::IsRetriableFailure(response.status());

    if (regular_op_state->GetCurrentConnection()) {
      regular_op_state->GetCurrentConnection()->UpdateErrorStatus(is_retriable_failure);
    }
    if (is_retriable_failure) {
      DEBUG_LOG("Recoverable error -- retrying");
      regular_op_state->ClearInternalRequest();
      regular_op_state->SetLastResult(response);
      HandleRequestGeneric(regular_op_state);
      return;
    }

    regular_op_state->Complete(response);

    int32_t pending_callbacks = regular_op_state->pending_callbacks.fetch_sub(1) - 1;
    assert(pending_callbacks >= 0);
    if (pending_callbacks == 0) {
      delete regular_op_state;
    }
  }

 private:
  ConnectionSet *FindConnectionSet(const std::string &endpoint) {
    std::shared_lock lock(conn_sets_mutex_);
    for (auto conn_set : conn_sets_) {
      if (conn_set->Contains(endpoint)) {
        return conn_set;
      }
    }
    return nullptr;
  }

  bool AddConnectionSet(ConnectionSet *new_conn_set) {
    std::unique_lock lock(conn_sets_mutex_);
    conn_sets_.push_back(new_conn_set);
    return true;
  }
};

MessageHandler *MessageHandlerFactory::Create(const std::string &implementation_name,
                                              const ObjectMapper *object_mapper,
                                              ConnectionManager *conn_manager) {
  if (implementation_name == "" || implementation_name == "basic") {
    return new BasicMessageHandler(object_mapper, conn_manager);
  } else if (implementation_name == "mock") {
    DEBUG_LOG("Creating MockMessageHandler");
    return new MockMessageHandler();
  }
  DEBUG_LOG("Unknown implementation: %s", implementation_name.c_str());
  return nullptr;
}

}  // namespace tfos::csdk
