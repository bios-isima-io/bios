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

/**
 * Logical Pipe Implementation
 */
#include "tfos-physical/impl/physical_pipe_impl.h"

#include <err.h>
#include <event2/bufferevent_ssl.h>
#include <event2/event.h>
#include <libgen.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <nghttp2/nghttp2.h>
#include <openssl/conf.h>
#include <openssl/crypto.h>
#include <openssl/err.h>
#include <openssl/opensslv.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "tfos-common/object_mapper.h"
#include "tfos-common/utils.h"
#include "tfos-physical/impl/http2_session.h"
#include "tfos-physical/impl/http_constants.h"
#include "tfos-physical/impl/http_resource_resolver.h"
#include "tfoscsdk/log.h"

namespace tfos::csdk {

#define MAKE_NV(NAME, VALUE)                                                                \
  {                                                                                         \
    reinterpret_cast<uint8_t *>(const_cast<char *>(NAME)),                                  \
        reinterpret_cast<uint8_t *>(const_cast<char *>((VALUE).c_str())), sizeof(NAME) - 1, \
        (VALUE).size(), NGHTTP2_NV_FLAG_NONE                                                \
  }

#define MAKE_NV_CHARVALUE(NAME, VALUE)                                            \
  {                                                                               \
    reinterpret_cast<uint8_t *>(const_cast<char *>(NAME)),                        \
        reinterpret_cast<uint8_t *>(const_cast<char *>(VALUE)), sizeof(NAME) - 1, \
        sizeof(VALUE) - 1, NGHTTP2_NV_FLAG_NONE                                   \
  }

#ifndef DEBUG_LOG_ENABLED
#define ASSERT_ON_EVENT_LOOP(pipe)
#else
#define ASSERT_ON_EVENT_LOOP(pipe) assert(pthread_self() == (pipe)->event_loop_->thread_id())
#endif

// PhysicalPipeImpl implementation
// ///////////////////////////////////////////////////////////////

void PhysicalPipeImpl::GlobalInit() {
  static bool done = false;
  if (!done) {
    Http2Session::GlobalInit();
    done = true;
  }
}

PhysicalPipeImpl::PhysicalPipeImpl(std::string host, uint16_t port, bool is_secure,
                                   const ResourceResolver *resource_resolver,
                                   const ObjectMapper *object_mapper)
    : host_(host),
      port_(port),
      is_secure_(is_secure),
      session_data_(nullptr),
      last_connection_error_(nullptr),
      event_loop_(new EventLoop()),
      resource_resolver_(resource_resolver),
      object_mapper_(object_mapper),
      shutdown_(false),
      shutdown_mutex_(nullptr),
      shutdown_cv_(nullptr) {}

void PhysicalPipeImpl::Initialize(response_cb caller_cb, void *caller_cb_args) {
  if (CreateSslContext(caller_cb, caller_cb_args)) {
    return;
  }
  auto *session_data = new Http2Session(this, host_, port_, cafile_, event_loop_, object_mapper_);
  auto *params =
      new Http2Session::InitiateConnectionParams(caller_cb, caller_cb_args, session_data);
  session_data->event_loop()->PutTask(Http2Session::InitiateConnection, params);
}

PhysicalPipeImpl::~PhysicalPipeImpl() {
  if (event_loop_ != nullptr) {
    event_loop_->Shutdown();
  }
  delete shutdown_cv_;
  delete shutdown_mutex_;
}

void PhysicalPipeImpl::Shutdown() {
  DEBUG_LOG("PhysicalPipeImpl::Shutdown %p host=%s", this, host_.c_str());
  shutdown_ = true;
  shutdown_mutex_ = new std::mutex();
  shutdown_cv_ = new std::condition_variable();

  if (event_loop_->IsValid()) {
    // This would free SSL context. The session termination above closes SSL via
    // event loop; Freeing SSl context has to be queued in the event loop after
    // the task to free SSL.
    event_loop_->PutFinalTask(TerminatePipe, this);

    // Block until the event loop is terminated
    while (ssl_ctx_ != nullptr) {
      std::unique_lock<std::mutex> lock(*shutdown_mutex_);
      shutdown_cv_->wait_for(lock, std::chrono::milliseconds(100));
    }
  } else {
    // Event loop wouldn't work. We just destroy the loop.
    delete event_loop_;
    event_loop_ = nullptr;
  }

  DEBUG_LOG("PhysicalPipeImpl::Shutdown %p host=%s unblocked", this, host_.c_str());
  delete this;
}

std::string PhysicalPipeImpl::GetLastConnectionError() {
  auto error = last_connection_error_;
  return error != nullptr ? *error : "";
}

static void DeleteMessageObject(void *arg) {
  auto message = reinterpret_cast<std::string *>(arg);
  delete message;
}

void PhysicalPipeImpl::SetConnectionError(const std::string &error_message) {
  ASSERT_ON_EVENT_LOOP(this);
  auto current_error = last_connection_error_;
  last_connection_error_ = new std::string(error_message);
  // someone may be referring to this object yet
  event_loop_->ScheduleTask(DeleteMessageObject, current_error, 1000);
}

void PhysicalPipeImpl::ClearConnectionError() {
  ASSERT_ON_EVENT_LOOP(this);
  auto current_error = last_connection_error_;
  last_connection_error_ = nullptr;
  // someone may be referring to this object yet
  event_loop_->ScheduleTask(DeleteMessageObject, current_error, 1000);
}

void PhysicalPipeImpl::TerminatePipe(void *args) {
  auto *pipe = reinterpret_cast<PhysicalPipeImpl *>(args);
  ASSERT_ON_EVENT_LOOP(pipe);
  DEBUG_LOG("PhysicalPipeImpl::TerminatePipe %p %s", pipe, pipe->host_.c_str());
  if (pipe->session_data_) {
    DEBUG_LOG("PhysicalPipe::TerminatePipe %p -- Sutting down session %p", pipe,
              pipe->session_data_);
    pipe->session_data_->Shutdown(NGHTTP2_CANCEL);
  }
  pipe->event_loop_->ScheduleTask(DiscardPipe, pipe, 0);
}

void PhysicalPipeImpl::DiscardPipe(void *args) {
  auto *pipe = reinterpret_cast<PhysicalPipeImpl *>(args);
  ASSERT_ON_EVENT_LOOP(pipe);

  auto *ssl_ctx = pipe->ssl_ctx_;
  if (ssl_ctx != nullptr) {
    DEBUG_LOG("PhysicalPipeImpl::DiscardPipe: free SSL context");
    SSL_CTX_free(ssl_ctx);
  }
  pipe->ssl_ctx_ = nullptr;

  DEBUG_LOG("PhysicalPipeImpl::DiscardPipe: %p %s signaling", pipe, pipe->host_.c_str());
  pipe->shutdown_cv_->notify_all();
}

int PhysicalPipeImpl::CreateSslContext(response_cb cb, void *cb_args) {
  // create and configure SSL context
#if OPENSSL_VERSION_NUMBER < 0x10100000L
  ssl_ctx_ = SSL_CTX_new(TLSv1_2_client_method());
#else
  ssl_ctx_ = SSL_CTX_new(TLS_client_method());
#endif
  if (ssl_ctx_ == nullptr) {
    return HandleSslConnectionError(cb, cb_args, "Could not create SSL/TLS context");
  }
  SSL_CTX_set_options(ssl_ctx_,
                      SSL_OP_ALL | SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 | SSL_OP_NO_COMPRESSION);

  SSL_CTX_set_alpn_protos(ssl_ctx_, (const unsigned char *)"\x02h2", 3);

  if (cafile_.empty()) {
    // Attempt to use the system's trusted root certificates.
    X509_STORE *store = SSL_CTX_get_cert_store(ssl_ctx_);
    if (X509_STORE_set_default_paths(store) != 1) {
      return HandleSslConnectionError(cb, cb_args, "Failed to set default cert paths");
    }
  } else if (SSL_CTX_load_verify_locations(ssl_ctx_, cafile_.c_str(), NULL) != 1) {
    return HandleSslConnectionError(cb, cb_args, "Failed to load cert file '" + cafile_ + "'");
  }
  return 0;
}

/**
 * Callback function invoked when the library wants to read data from the
 * source. The read data is sent in the stream stream_id. The implementation of
 * this function must read at most datamax bytes of data from source (or
 * possibly other places) and store them in buf and return number of data stored
 * in buf. If EOF is reached, set NGHTTP2_DATA_FLAG_EOF flag in *data_flags.
 *
 * Sometime it is desirable to avoid copying data into buf and let application
 * to send data directly. To achieve this, set NGHTTP2_DATA_FLAG_NO_COPY to
 * *data_flags (and possibly other flags, just like when we do copy), and return
 * the number of bytes to send without copying data into buf. The library,
 * seeing NGHTTP2_DATA_FLAG_NO_COPY, will invoke nghttp2_send_data_callback.
 * The application must send complete DATA frame in that callback.
 */
static ssize_t data_source_read_callback(nghttp2_session *session, int32_t stream_id, uint8_t *buf,
                                         size_t datamax, uint32_t *data_flags,
                                         nghttp2_data_source *source, void *user_data) {
  auto *stream_data =
      reinterpret_cast<Http2Stream *>(nghttp2_session_get_stream_user_data(session, stream_id));
  *data_flags = NGHTTP2_DATA_FLAG_NO_COPY;

  if (datamax == 0) {
    DEBUG_LOG("data_source_read_callback -- deferred; pos=%ld", stream_data->data_position);
    return NGHTTP2_ERR_DEFERRED;
  }

  ssize_t data_size = std::min(stream_data->request_size - stream_data->data_position,
                               static_cast<ssize_t>(datamax));

  // Put EOF if this is the last frame
  if (stream_data->data_position + data_size == stream_data->request_size) {
    *data_flags |= NGHTTP2_DATA_FLAG_EOF;
  }

  DEBUG_LOG("data_source_read_callback; length=%ld flags=%d position=%ld", data_size, *data_flags,
            stream_data->data_position);
  return data_size;
}

/**
 * Send HTTP request to the remote peer.
 *
 * This method would run on the caller's thread (application thread). The method
 * creates a stream data object and populate necessary information for executing
 * the operation. Then the method puts a task to run InvokeStream into the
 * EventLoop.
 *
 * Note that InvokeStream task runs on an EventLoop thread after this method
 * terminates. No local variables in this method must not be used in the task.
 * You must copy them to carry over the task. Tricky part is MAKE_NV. This macro
 * takes the data pointer of the std::string to build a header field entry. Its
 * contents are still owned by the orignal std::string instances. Assining a
 * local std::string to headers via MAKE_NV would cause segmentation violation
 * during the task execution.
 */
void PhysicalPipeImpl::HandleRequest(const RequestMessage &request) {
  if (session_data_ == nullptr) {
    // The HTTP2 session has not been established yet.
    std::string endpoint = host() + ":" + std::to_string(port());
    ResponseMessage resp(OpStatus::SERVER_CONNECTION_FAILURE, endpoint, {0});
    request.context()->completion_cb(resp, request.context()->completion_cb_args);
    return;
  }
  // Create stream data
  const std::string method = resource_resolver_->GetMethod(request.op_id());
  const std::string resource =
      resource_resolver_->GetResource(request.op_id(), request.context()->resources);
  const std::string content_type = resource_resolver_->GetContentType(request.op_id());
  Http2Stream *stream_data =
      new Http2Stream(session_data_, method, resource, content_type, request.req_message_length(),
                      request.context()->timeout, request.context()->completion_cb,
                      request.context()->completion_cb_args, IsOperationBulk(request.op_id()));

  // Build headers
  static const std::string scheme = "https";  // always the case in this class
  std::vector<nghttp2_nv> &headers = stream_data->headers;
  headers.push_back(MAKE_NV(kHeaderNameMethod, stream_data->method));
  headers.push_back(MAKE_NV(kHeaderNameScheme, scheme));
  headers.push_back(MAKE_NV(kHeaderNameAuthority, session_data_->authority()));
  headers.push_back(MAKE_NV(kHeaderNamePath, stream_data->path));
  headers.push_back(MAKE_NV(kHeaderNameContentType, stream_data->content_type));
  headers.push_back(MAKE_NV(kHeaderNameClientVersion, stream_data->client_version));

  if (Utils::IsProtobufOperation(request.op_id())) {
    // do this only for protobuf for now to avoid breakages
    headers.push_back(MAKE_NV(kHeaderNameAccept, stream_data->content_type));
  }
  if (request.context()->is_metrics_logging) {
    headers.push_back(MAKE_NV_CHARVALUE(kHeaderNameInternalRequest, kHeaderValueMetrics));
  }
  if (request.req_message() != nullptr) {
    headers.push_back(MAKE_NV(kHeaderNameContentLength, stream_data->request_size_string));
    stream_data->provider.source.ptr = const_cast<uint8_t *>(request.req_message());
    stream_data->provider.read_callback = data_source_read_callback;
    stream_data->p_provider = &stream_data->provider;
  }
  if (request.context() && request.context()->token) {
    headers.push_back(MAKE_NV(kHeaderNameAuthorization, *request.context()->token));
  }

  if (request.context()) {
    stream_data->timestamps = request.context()->metric_timestamps;
  }

  // Put the task to invoke the stream
  event_loop_->PutTask(InvokeStream, stream_data);
}

/**
 * EventLoop task to invoke a stream.
 */
void PhysicalPipeImpl::InvokeStream(void *args) {
  auto *stream_data = reinterpret_cast<Http2Stream *>(args);
  auto *session_data = stream_data->session_data;

  if (session_data->pipe()->shutdown_) {
    std::stringstream msg;
    msg << "Connection is shutting down; endpoint=" << session_data->authority();
    session_data->ReportError(OpStatus::OPERATION_CANCELLED, msg.str(), stream_data);
    delete stream_data;
    return;
  }

  if (session_data->terminated()) {
    std::stringstream msg;
    msg << "Session has been terminated; method=" << stream_data->method
        << ", path=" << stream_data->path << ", endpoint=" << session_data->authority();
    DEBUG_LOG("%s", msg.str().c_str());
    session_data->ReportError(OpStatus::SERVER_CHANNEL_ERROR, msg.str(), stream_data);
    return;
  }

  // check request size
  DEBUG_LOG("request size=%ld", stream_data->request_size);
  const ssize_t kMaxSize = 1000000;
  if (stream_data->request_size > kMaxSize) {
    std::stringstream msg;
    msg << "Request body size is " << stream_data->request_size << " that exceeds maximum "
        << kMaxSize;
    DEBUG_LOG("%s", msg.str().c_str());
    session_data->ReportError(OpStatus::REQUEST_TOO_LARGE, msg.str(), stream_data);
    return;
  }

  std::vector<nghttp2_nv> &headers = stream_data->headers;
  int32_t stream_id = nghttp2_submit_request(session_data->session(), nullptr, headers.data(),
                                             headers.size(), stream_data->p_provider, stream_data);
  if (stream_id < 0) {
    const char *error_string = nghttp2_strerror(stream_id);
    std::stringstream msg;
    msg << "Could not submit HTTP request; method=" << stream_data->method
        << ", path=" << stream_data->path << ", endpoint=" << session_data->authority()
        << ", error_code=" << stream_id << ", error=" << error_string;
    DEBUG_LOG("%s", msg.str().c_str());
    session_data->ReportError(OpStatus::SERVER_CHANNEL_ERROR, msg.str(), stream_data);
    return;
  }
  DEBUG_LOG("request submitted; stream_id=%d, headers:", stream_id);
  PRINT_HEADERS(headers);

  stream_data->stream_id = stream_id;
  if (stream_data->timeout_millis > 0) {
    stream_data->timeout_handler = new TimeoutHandler(stream_data, stream_data->timeout_millis);
  }
  session_data->AttachStream(stream_id);

  int rv = nghttp2_session_send(session_data->session());
  if (rv == 0) {
    if (stream_data->timeout_handler != nullptr) {
      stream_data->timeout_handler->Start();
    }
  } else {
    session_data->AbortStream(OpStatus::CLIENT_CHANNEL_ERROR, "Could not send data to HTTP session",
                              rv, NGHTTP2_INTERNAL_ERROR, stream_data);
  }
  // REQUEST_SUBMIT
  Utils::PutTimestamp(stream_data->timestamps);
}

void PhysicalPipeImpl::AttachSession(Http2Session *session_data) {
  if (session_data_) {
    session_data_->Discard(0);
  }
  session_data_ = session_data;
}

void PhysicalPipeImpl::DetachSession(Http2Session *session_data) {
  ASSERT_ON_EVENT_LOOP(this);
  if (session_data_ == session_data) {
    session_data_ = nullptr;
  }
}

int PhysicalPipeImpl::HandleSslConnectionError(response_cb cb, void *cb_args, std::string message) {
  message += ": ";
  message += ERR_error_string(ERR_get_error(), nullptr);
  payload_t reply = object_mapper_->WriteValue<ErrorResponse>(ErrorResponse(message));
  cb(ResponseMessage(OpStatus::SERVER_CONNECTION_FAILURE, "", reply), cb_args);
  return -1;
}

/**
 * This method handles a disconnection event. The method is supposed to run on
 * the event loop on receiving a connection error
 */
void PhysicalPipeImpl::HandleDisconnection(Http2Session *session_data) {
  DEBUG_LOG("PhysicalPipeImpl::HandleDisconnection session_data=%p", session_data);
  ASSERT_ON_EVENT_LOOP(this);

  DetachSession(session_data_);

  if (!shutdown_) {
    Http2Session *new_session_data =
        new Http2Session(this, host_, port_, cafile_, event_loop_, object_mapper_);
    DEBUG_LOG("Created a new session for reconnection from pipe: %p", new_session_data);
    new_session_data->InitiateReconnection();
  }
}

}  // namespace tfos::csdk
