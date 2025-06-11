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
#include "tfos-physical/impl/http2_session.h"

#include <err.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <openssl/conf.h>
#include <openssl/crypto.h>
#include <openssl/err.h>
#include <openssl/opensslv.h>
#include <openssl/x509v3.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>

#include "tfos-common/event_loop.h"
#include "tfos-common/logger.h"
#include "tfos-common/object_mapper.h"
#include "tfos-physical/impl/http_response_handler.h"
#include "tfos-physical/impl/physical_pipe_impl.h"

// #include "tfos-physical/impl/physical_pipe_impl.h"

namespace tfos::csdk {

#define ARRLEN(T, x) ((T)(sizeof(x) / sizeof(x[0])))

#ifndef DEBUG_LOG_ENABLED
#define ASSERT_ON_EVENT_LOOP(session_data)
#else
#define ASSERT_ON_EVENT_LOOP(session_data) \
  assert(pthread_self() == (session_data)->event_loop_->thread_id())
#endif

// TimeoutHandler implementation
// /////////////////////////////////////////////////////////////////

TimeoutHandler::TimeoutHandler(Http2Stream *stream_data, int64_t timeout_millis) {
  assert(stream_data != nullptr);
  session_data_ = stream_data->session_data;
  stream_data_ = stream_data;
  stream_id_ = stream_data->stream_id;
  event_base_ = session_data_->event_loop()->event_base();
  ev_ = nullptr;
  timeout_millis_ = timeout_millis;
  delay_.tv_sec = timeout_millis / 1000;
  delay_.tv_usec = timeout_millis % 1000 * 1000;
  DEBUG_LOG("TimeoutHandler::TimeoutHandler -- timeout=%ld", timeout_millis_);
}

TimeoutHandler::~TimeoutHandler() { Stop(); }

void TimeoutHandler::Start() {
  ev_ = evtimer_new(event_base_, TimeoutCallback, this);
  evtimer_add(ev_, &delay_);
}

void TimeoutHandler::Stop() {
  if (ev_ != nullptr) {
    evtimer_del(ev_);
    event_free(ev_);
    ev_ = nullptr;
  }
}

void TimeoutHandler::TimeoutCallback(evutil_socket_t fd, int16_t what, void *args) {
  auto *timeout_handler = reinterpret_cast<TimeoutHandler *>(args);
  auto *session_data = timeout_handler->session_data_;
  int32_t stream_id = timeout_handler->stream_id_;
  DEBUG_LOG("TimeoutHandler::TimeoutCallback -- stream_id=%d", stream_id);

  if (session_data->HasStream(stream_id)) {
    auto *stream_data = timeout_handler->stream_data_;

    int error = nghttp2_submit_rst_stream(session_data->session(), 0, stream_id, NGHTTP2_CANCEL);
    if (error) {
      session_data->AbortStream(OpStatus::CLIENT_CHANNEL_ERROR, "Could not reset stream on timeout",
                                error, NGHTTP2_INTERNAL_ERROR, stream_data);
    } else {
      std::stringstream msg;
      msg << "Operation timed out; method=" << stream_data->method << ", path=" << stream_data->path
          << ", endpoint=" << session_data->authority()
          << ", timeout=" << timeout_handler->timeout_millis_;
      session_data->ReportError(OpStatus::TIMEOUT, msg.str(), stream_data);
      session_data->DetachStream(stream_id);

      event_free(timeout_handler->ev_);
      timeout_handler->ev_ = nullptr;
      delete stream_data;
    }
  }
}

// Http2Session implementation
// /////////////////////////////////////////////////////////

#ifdef DEBUG_LOG_ENABLED
void Http2Session::PrintHeader(FILE *f, const uint8_t *name, size_t namelen, const uint8_t *value,
                               size_t valuelen) {
  fprintf(f, "  ");
  fwrite(name, 1, namelen, f);
  fprintf(f, ": ");
  fwrite(value, 1, valuelen, f);
  fprintf(f, "\n");
}

/* Print HTTP headers to |f|. Please note that this function does not
   take into account that header name and value are sequence of
   octets, therefore they may contain non-printable characters. */
void Http2Session::PrintHeaders(FILE *f, const std::vector<nghttp2_nv> &nvas) {
  for (auto nva : nvas) {
    PrintHeader(f, nva.name, nva.namelen, nva.value, nva.valuelen);
  }
  fprintf(f, "\n");
}
#endif

void Http2Session::GlobalInit() {
#if OPENSSL_VERSION_NUMBER < 0x10100000L
  SSL_library_init();
  SSL_load_error_strings();
#else
  OPENSSL_init_ssl(0, nullptr);
#endif
}

Http2Session::Http2Session(PhysicalPipeImpl *pipe, const std::string &host, uint16_t port,
                           const std::string &cafile, EventLoop *event_loop,
                           const ObjectMapper *object_mapper)
    : pipe_(pipe), event_loop_(event_loop), object_mapper_(object_mapper), magic_(0) {
  session_ = nullptr;
  bev_ = nullptr;
  host_ = host;
  port_ = port;
  authority_ = host + ":" + std::to_string(port);
  cafile_ = cafile;
  ssl_ctx_ = pipe_->ssl_ctx();
  terminated_ = false;
  shutdown_reason_ = 0;
  connection_count_ = 0;
  reconnection_delay_millis_ = 0;
  conn_cb_ = nullptr;
  conn_cb_args_ = nullptr;
}

Http2Session::~Http2Session() {
  DEBUG_LOG("Http2Session::~Http2Session this=%p, authority=%s", this, authority_.c_str());
  magic_ = 0xdeadbeef;
}

/**
 * Method to shutdown the session.
 *
 * In order to shutdown the session gracefully, we don't delete the session
 * immediately. This method is supposed to be called by an event loop. Any tasks
 * for this session may be queued already. This method defers the task to
 * terminate the session in order to allow these queued tasks being consumed
 * safely.
 */
void Http2Session::Shutdown(uint32_t reason) {
  DEBUG_LOG("Http2Session::Shutdown %p", this);
  ASSERT_ON_EVENT_LOOP(this);

  if (terminated_) {
    DEBUG_LOG("shutdown has been initialted already");
    return;
  }

  // Send a GOAWAY frame if the HTTP2 session is estabilished already
  if (session() != nullptr) {
    int ret = nghttp2_session_terminate_session(session(), reason);
    DEBUG_LOG("terminate_session result=%s (%d)", nghttp2_strerror(ret), ret);
    (void)ret;
  }

  terminated_ = true;
  shutdown_reason_ = reason;

  pipe_->DetachSession(this);

  DEBUG_LOG("Putting task RunShutdown session_data=%p", this);
  event_loop_->PutTask(RunShutdown, this);
}

void Http2Session::RunShutdown(void *args) {
  auto *session_data = reinterpret_cast<Http2Session *>(args);
  DEBUG_LOG("Http2Session::RunShutdown %p", session_data);
  ASSERT_ON_EVENT_LOOP(session_data);
  if (session_data->session() != nullptr) {
    while (!session_data->stream_ids_.empty()) {
      on_stream_close_callback(session_data->session(), *session_data->stream_ids_.begin(),
                               session_data->shutdown_reason_, session_data);
    }
    DEBUG_LOG("Http2Session::RunShutdown free H2 session");
    nghttp2_session_del(session_data->session_);
  }

  auto bev_to_free = session_data->bev_;
  if (bev_to_free != nullptr) {
    DEBUG_LOG("Http2Session::RunShutdown free H2 event");
    bufferevent_setcb(bev_to_free, nullptr, nullptr, nullptr, nullptr);
    session_data->bev_ = nullptr;
    bufferevent_disable(bev_to_free, EV_READ | EV_WRITE);
    bufferevent_free(bev_to_free);
  }
  session_data->Discard(100);
}

void Http2Session::Discard(uint64_t delay_millis) {
  if (delay_millis == 0) {
    event_loop_->PutTask(RunDiscard, this);
  } else {
    event_loop_->ScheduleTask(RunDiscard, this, delay_millis);
  }
}

void Http2Session::RunDiscard(void *args) {
  auto *session_data = reinterpret_cast<Http2Session *>(args);
  DEBUG_LOG("Http2Session::RunDiscard %p", session_data);
  delete session_data;
}

/**
 * Method to handle a connection error.
 *
 * The behavior of this method follows the description of RFC7540 Section 5.4.1.
 * The method first sends GOAWAY frame, terminate session, and disconnect.
 */
void Http2Session::HandleConnectionError(uint32_t reason) {
  DEBUG_LOG("Http2Session::HandleConnectionError session=%p, reason=%d", this, reason);
  ASSERT_ON_EVENT_LOOP(this);
  pipe_->HandleDisconnection(this);
  Shutdown(reason);
}

void Http2Session::DetachPipe() { pipe_ = nullptr; }

void Http2Session::AttachStream(int32_t stream_id) {
  DEBUG_LOG("stream=%p attaching stream; stream_id=%d", this, stream_id);
  ASSERT_ON_EVENT_LOOP(this);
  stream_ids_.insert(stream_id);
}

void Http2Session::DetachStream(int32_t stream_id) {
  DEBUG_LOG("stream=%p detaching stream; stream_id=%d", this, stream_id);
  ASSERT_ON_EVENT_LOOP(this);
  stream_ids_.erase(stream_id);
}

bool Http2Session::HasStream(int32_t stream_id) {
  ASSERT_ON_EVENT_LOOP(this);
  return stream_ids_.find(stream_id) != stream_ids_.end();
}

bool Http2Session::HasPendingStreams() {
  ASSERT_ON_EVENT_LOOP(this);
  return !stream_ids_.empty();
}

/**
 * Returns whether the session is ready.
 */
bool Http2Session::IsReady() { return bev_ != nullptr; }

/**
 * Initiates a SSL connection to the endpoint and start an nghttp2 session.
 * @param cb Connection callback
 */
void Http2Session::InitiateConnection(void *args) {
  auto *params = reinterpret_cast<InitiateConnectionParams *>(args);
  auto cb = params->cb;
  auto cb_args = params->cb_args;
  auto session_data = params->session_data;
  delete params;
  DEBUG_LOG("Http2Session::InitiateConnection %p, authority=%s", session_data,
            session_data->authority_.c_str());

  if (session_data->bev_ != nullptr || session_data->pipe()->shutdown()) {
    // Another concurrent operation has initiated the connection already, or
    // shutting down
    auto response_message = ResponseMessage(OpStatus::OK, session_data->authority(), {0});
    cb(response_message, cb_args);
    delete session_data;
    return;
  }

  // create an SSL session and put it to an buffer event.
  SSL *ssl = SSL_new(session_data->ssl_ctx_);
  if (ssl == nullptr) {
    session_data->pipe_->HandleSslConnectionError(cb, cb_args,
                                                  "Could not create SSL/TLS session object");
    return;
  }
  SSL_set_tlsext_host_name(ssl, session_data->host_.c_str());

  session_data->conn_cb_ = cb;
  session_data->conn_cb_args_ = cb_args;
  struct bufferevent *bev = bufferevent_openssl_socket_new(session_data->event_loop_->event_base(),
                                                           -1, ssl, BUFFEREVENT_SSL_CONNECTING,
                                                           BEV_OPT_CLOSE_ON_FREE);
  session_data->bev_ = bev;
  bufferevent_setcb(bev, readcb_connect, writecb_connect, eventcb_connect, session_data);

  if (session_data->connection_count_ == 0) {
    DEBUG_LOG("putting InvokeSession task with args %p", cb_args);
    session_data->event_loop_->PutTask(InvokeSession, session_data);
    session_data->reconnection_delay_millis_ = 100;
  } else {
    DEBUG_LOG("scheduling InvokeSession task with args %p delay=%ld", cb_args,
              session_data->reconnection_delay_millis_);
    session_data->event_loop_->ScheduleTask(InvokeSession, session_data,
                                            session_data->reconnection_delay_millis_);
    if (session_data->reconnection_delay_millis_ <= 10000) {
      session_data->reconnection_delay_millis_ *= 2;
    }
    DEBUG_LOG("scheduling InvokeSession task with args %p delay=%ld REVISED", cb_args,
              session_data->reconnection_delay_millis_);
  }
  ++session_data->connection_count_;
}

void Http2Session::InitiateReconnection() {
  DEBUG_LOG("Http2Session::InitiateReconnection %p, authority=%s", this, authority_.c_str());
  InitiateConnectionParams *params = new InitiateConnectionParams(CompleteReconnection, this, this);
  // TODO(Mayur): _clientMetrics Reconnecting
  event_loop_->PutTask(InitiateConnection, params);
}

/**
 * Retrieve reconnection parameters of the previous failed session and set new
 * parameters for this session.
 */
void Http2Session::SetReconnection(Http2Session *previous) {
  connection_count_ = previous->connection_count_ + 1;
  if (previous->reconnection_delay_millis_ == 0) {
    reconnection_delay_millis_ = 100;
  } else if (previous->reconnection_delay_millis_ < 10000) {
    // exponentially back off up to 10 seconds
    reconnection_delay_millis_ = previous->reconnection_delay_millis_ * 2;
  }
  // TODO(Naoki): Should we give up after certain number of retries?
}

/**
 * Tells the event base to start the connection task.
 */
void Http2Session::InvokeSession(void *args) {
  auto *session_data = reinterpret_cast<Http2Session *>(args);
  std::string endpoint = session_data->host_ + ":" + std::to_string(session_data->port_);
  std::string error_message = "Connection failure; host=" + endpoint;
  if (session_data->pipe_->shutdown()) {
    // we don't attempt connection
    payload_t reply = session_data->object_mapper_->WriteValue<ErrorResponse>(
        ErrorResponse("Session terminated"));
    session_data->conn_cb_(ResponseMessage(OpStatus::SERVER_CONNECTION_FAILURE, endpoint, reply),
                           session_data->conn_cb_args_);
    return;
  }

  // The synchrnous getaddressinfo may occupy the event loop for long time in
  // case the DNS takes long time to respond, but it is OK since resolving the
  // address is a blocker of the event loop. The PysicalPipe uses one event loop
  // per connection; any other tasks cannot proceed util we resolve the peer
  // address.
  //
  // TODO(Naoki): We may want to cache the address.
  struct addrinfo hints = {0};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  struct addrinfo *addrinfo = nullptr;
  int result = getaddrinfo(session_data->host_.c_str(), nullptr, &hints, &addrinfo);
  DEBUG_LOG("GETADDRINFO; host=%s, result=%d", session_data->host_.c_str(), result);
  if (result != 0) {
    std::stringstream msg;
    msg << "Resolving address failed for host " << session_data->host_ << ": (" << result << ") "
        << gai_strerror(result);
    DEBUG_LOG("%s", msg.str().c_str());
    payload_t reply =
        session_data->object_mapper_->WriteValue<ErrorResponse>(ErrorResponse(msg.str()));
    session_data->conn_cb_(ResponseMessage(OpStatus::SERVER_CONNECTION_FAILURE, endpoint, reply),
                           session_data->conn_cb_args_);
    return;
  }

  bufferevent_enable(session_data->bev_, EV_READ | EV_WRITE);

  result = -1;
  for (auto current = addrinfo; result != 0 && current != nullptr; current = current->ai_next) {
    auto sin = reinterpret_cast<struct sockaddr_in *>(current->ai_addr);
    sin->sin_port = htons(session_data->port_);
    // make connection
    result = bufferevent_socket_connect(session_data->bev_,
                                        reinterpret_cast<struct sockaddr *>(sin), sizeof(*sin));
  }
  freeaddrinfo(addrinfo);
  if (result != 0) {
    payload_t reply =
        session_data->object_mapper_->WriteValue<ErrorResponse>(ErrorResponse(error_message));
    session_data->conn_cb_(ResponseMessage(OpStatus::SERVER_CONNECTION_FAILURE, endpoint, reply),
                           session_data->conn_cb_args_);
  }
}

void Http2Session::AbortStream(OpStatus status, const std::string &message, int rcv_error,
                               int32_t send_error, Http2Stream *stream_data) {
  std::stringstream msg;
  msg << message << "; method=" << stream_data->method << ", path=" << stream_data->path
      << ", endpoint=" << authority();
  if (rcv_error != 0) {
    const char *error_string =
        rcv_error > 0 ? nghttp2_http2_strerror(rcv_error) : nghttp2_strerror(rcv_error);
    msg << ", error_code=" << rcv_error << ", error=" << error_string;
  }
  DEBUG_LOG("%s", msg.str().c_str());
  ReportError(status, msg.str(), stream_data);
  stream_data->error_code = send_error;
  event_loop_->PutTask(RunAbortStream, stream_data);
}

void Http2Session::ReportError(OpStatus status, const std::string &error_message,
                               Http2Stream *stream_data) {
  payload_t reply = object_mapper_->WriteValue<ErrorResponse>(ErrorResponse(error_message));
  auto response_message = ResponseMessage(status, authority(), reply);
  stream_data->caller_cb(response_message, stream_data->caller_cb_args);
}

/**
 * Send a RST_STREAM frame to the peer, detach the stream from the session, and
 * delete the stream.
 */
void Http2Session::RunAbortStream(void *args) {
  auto *stream_data = reinterpret_cast<Http2Stream *>(args);
  auto *session_data = stream_data->session_data;
  nghttp2_submit_rst_stream(session_data->session(), 0, stream_data->stream_id,
                            stream_data->error_code);
  session_data->DetachStream(stream_data->stream_id);
  delete stream_data;
}

void Http2Session::SetHttp2Callbacks() {
  nghttp2_session_callbacks *callbacks;

  nghttp2_session_callbacks_new(&callbacks);

  nghttp2_session_callbacks_set_send_callback(callbacks, send_callback);

  nghttp2_session_callbacks_set_send_data_callback(callbacks, send_data_callback);

  nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks, on_frame_recv_callback);

  nghttp2_session_callbacks_set_on_data_chunk_recv_callback(callbacks, on_data_chunk_recv_callback);

  nghttp2_session_callbacks_set_on_stream_close_callback(callbacks, on_stream_close_callback);

  nghttp2_session_callbacks_set_on_header_callback(callbacks, on_header_callback);

  nghttp2_session_callbacks_set_on_begin_headers_callback(callbacks, on_begin_headers_callback);

  nghttp2_session_client_new(&session_, callbacks, this);

  nghttp2_session_callbacks_del(callbacks);
}

/* nghttp2_send_callback. Here we transmit the |data|, |length| bytes,
   to the network. Because we are using libevent bufferevent, we just
   write those bytes into bufferevent buffer. */
ssize_t Http2Session::send_callback(nghttp2_session *session, const uint8_t *data, size_t length,
                                    int flags, void *user_data) {
  auto *session_data = reinterpret_cast<Http2Session *>(user_data);
  struct bufferevent *bev = session_data->bev_;
  (void)session;
  (void)flags;

  if (bev != nullptr) {
    bufferevent_write(bev, data, length);
  }
  DEBUG_LOG("send_callback returning %ld", length);
  return (ssize_t)length;
}

/* Callback function invoked when :enum:`NGHTTP2_DATA_FLAG_NO_COPY` is
   used in :type:`nghttp2_data_source_read_callback` to send complete
   DATA frame. */
int Http2Session::send_data_callback(nghttp2_session *session, nghttp2_frame *frame,
                                     const uint8_t *framehd, size_t length,
                                     nghttp2_data_source *source, void *user_data) {
  auto *session_data = reinterpret_cast<Http2Session *>(user_data);
  auto *stream_data = reinterpret_cast<Http2Stream *>(
      nghttp2_session_get_stream_user_data(session, frame->hd.stream_id));
  DEBUG_LOG("send_data_callback stream=%p length=%ld", stream_data, length);

  struct bufferevent *bev = session_data->bev_;
  bufferevent_write(bev, framehd, 9);
  if (frame->data.padlen > 0) {
    uint8_t padlen = frame->data.padlen - 1;
    bufferevent_write(bev, &padlen, 1);
  }
  bufferevent_write(bev, reinterpret_cast<uint8_t *>(source->ptr) + stream_data->data_position,
                    length);
  stream_data->data_position += length;
  if (frame->data.padlen > 1) {
    uint8_t pad[frame->data.padlen - 1] = {0};
    bufferevent_write(bev, pad, frame->data.padlen - 1);
  }
  DEBUG_LOG("send_data_callback stream=%p length=%ld -- done", stream_data, length);
  return 0;
}

/* nghttp2_on_header_callback: Called when nghttp2 library emits
   single header name/value pair. */
int Http2Session::on_header_callback(nghttp2_session *session, const nghttp2_frame *frame,
                                     const uint8_t *name, size_t namelen, const uint8_t *value,
                                     size_t valuelen, uint8_t flags, void *user_data) {
  auto *session_data = reinterpret_cast<Http2Session *>(user_data);

  static const std::string kNameStatus = ":status";
  static const std::string kNameContentLength = "content-length";

  switch (frame->hd.type) {
    case NGHTTP2_HEADERS:
      if (session_data->HasStream(frame->hd.stream_id)) {
        auto *stream_data = static_cast<Http2Stream *>(
            nghttp2_session_get_stream_user_data(session, frame->hd.stream_id));
        if (frame->headers.cat == NGHTTP2_HCAT_RESPONSE && stream_data != nullptr) {
          PRINT_HEADER(name, namelen, value, valuelen);
          std::string name_string = std::string((const char *)name, namelen);
          std::string value_string = std::string((const char *)value, valuelen);
          if (name_string == kNameStatus) {
            stream_data->status = atoi(value_string.c_str());
          } else {
            stream_data->response_headers[name_string] = value_string;
            if (name_string == kNameContentLength) {
              stream_data->response_capacity = atoi(value_string.c_str());
              stream_data->response_body =
                  Utils::AllocatePayload(stream_data->response_capacity + 1).data;
              DEBUG_LOG("Created payload memory %p", stream_data->response_body);
              stream_data->response_pos = 0;
            }
          }
        }
      }
      break;
  }
  return 0;
}

/* nghttp2_on_begin_headers_callback: Called when nghttp2 library gets
   started to receive header block. */
int Http2Session::on_begin_headers_callback(nghttp2_session *session, const nghttp2_frame *frame,
                                            void *user_data) {
  Http2Session *session_data = static_cast<Http2Session *>(user_data);
  if (!session_data->HasStream(frame->hd.stream_id)) {
    DEBUG_LOG("on_begin_headers_callback; stream_id=%d is canceled already", frame->hd.stream_id);
    return 0;
  }

  auto *stream_data = reinterpret_cast<Http2Stream *>(
      nghttp2_session_get_stream_user_data(session, frame->hd.stream_id));

  if (stream_data != nullptr) {
    // SERVER_PROCESS
    Utils::PutTimestamp(stream_data->timestamps);
  }
  switch (frame->hd.type) {
    case NGHTTP2_HEADERS: {
      if (frame->headers.cat == NGHTTP2_HCAT_RESPONSE && stream_data != nullptr) {
        DEBUG_LOG("Response headers for stream_id=%d:", frame->hd.stream_id);
      }
      break;
    }
  }
  return 0;
}

/* nghttp2_on_frame_recv_callback: Called when nghttp2 library
   received a complete frame from the remote peer. */
int Http2Session::on_frame_recv_callback(nghttp2_session *session, const nghttp2_frame *frame,
                                         void *user_data) {
  Http2Session *session_data = static_cast<Http2Session *>(user_data);
#if defined(DEBUG_LOG_ENABLED)
  static const char *frame_type[] = {"DATA",          "HEADERS",      "PRIORITY", "RST_STREAM",
                                     "SETTINGS",      "PUSH_PROMISE", "PING",     "GOAWAY",
                                     "WINDOW_UPDATE", "CONTINUATION"};
  static const char *settings_param[] = {"n/a",
                                         "SETTINGS_HEADER_TABLE_SIZE",
                                         "SETTINGS_ENABLE_PUSH",
                                         "SETTINGS_MAX_CONCURRENT_STREAMS",
                                         "SETTINGS_INITIAL_WINDOW_SIZE",
                                         "SETTINGS_MAX_FRAME_SIZE",
                                         "SETTINGS_MAX_HEADER_LIST_SIZE",
                                         "n/a",
                                         "SETTINGS_ENABLE_CONNECT_PROTOCOL"};
#endif
  int type = frame->hd.type;
  DEBUG_LOG("Frame received; type=%s",
            (type >= 0 && type < ARRLEN(int, frame_type)) ? frame_type[type] : "UNKNOWN");
  switch (type) {
#if defined(DEBUG_LOG_ENABLED)
    case NGHTTP2_HEADERS: {
      if (session_data->HasStream(frame->hd.stream_id)) {
        auto *stream_data = static_cast<Http2Stream *>(
            nghttp2_session_get_stream_user_data(session, frame->hd.stream_id));
        if (frame->headers.cat == NGHTTP2_HCAT_RESPONSE && stream_data != nullptr) {
          DEBUG_LOG("All headers received");
        }
        break;
      }
    }
    case NGHTTP2_SETTINGS:
      for (uint64_t i = 0; i < frame->settings.niv; ++i) {
        int32_t settings_id = frame->settings.iv[i].settings_id;
        if (settings_id < 0 || settings_id >= ARRLEN(int32_t, settings_param)) {
          settings_id = 0;
        }
        DEBUG_LOG("  %s: %d", settings_param[settings_id], frame->settings.iv[i].value);
      }
      break;
#endif
    case NGHTTP2_GOAWAY: {
      uint32_t error_code = frame->goaway.error_code;
      DEBUG_LOG("  last_stream_id : %d", frame->goaway.last_stream_id);
      DEBUG_LOG("  error_code     : %d (%s)", error_code, nghttp2_http2_strerror(error_code));
      session_data->pipe_->SetConnectionError("Disconnected by peer");
      session_data->HandleConnectionError(error_code ? error_code : NGHTTP2_CANCEL);
      break;
    }
  }
  return 0;
}

/* nghttp2_on_data_chunk_recv_callback: Called when DATA frame is
   received from the remote peer. In this implementation, if the frame
   is meant to the stream we initiated, print the received data in
   stdout, so that the user can redirect its output to the file
   easily. */
int Http2Session::on_data_chunk_recv_callback(nghttp2_session *session, uint8_t flags,
                                              int32_t stream_id, const uint8_t *data, size_t len,
                                              void *user_data) {
  auto *session_data = static_cast<Http2Session *>(user_data);
  if (!session_data->HasStream(stream_id)) {
    return 0;
  }
  auto *stream_data =
      reinterpret_cast<Http2Stream *>(nghttp2_session_get_stream_user_data(session, stream_id));
  if (stream_data != nullptr) {
    DEBUG_LOG(
        "on_data_chunk_recv_callback stream_id=%d data=%p len=%ld cap=%ld "
        "pos=%d",
        stream_id, data, len, stream_data->response_capacity, stream_data->response_pos);
    // extend the body if necessary
    if (stream_data->response_capacity - stream_data->response_pos < len + 1) {
      // calculate the minimum power of 2 that is larger than the required size
      size_t new_capacity = stream_data->response_pos + len + 1;
      for (size_t temp = new_capacity; temp > 0; temp >>= 1) {
        new_capacity |= temp;
      }
      ++new_capacity;
      DEBUG_LOG("extending response_body capacity from %ld to %ld (pos=%d len=%ld)",
                stream_data->response_capacity, new_capacity, stream_data->response_pos, len);
      uint8_t *prev_data = stream_data->response_body;
      stream_data->response_body = Utils::AllocatePayload(new_capacity).data;
      if (prev_data != nullptr) {
        memcpy(stream_data->response_body, prev_data, stream_data->response_pos);
      }
      Utils::ReleasePayload(prev_data);
      stream_data->response_capacity = new_capacity;
    }
    assert(len == 0 || stream_data->response_body != nullptr);
    assert(stream_data->response_pos + len <= stream_data->response_capacity);
    if (len > 0) {
      memcpy(stream_data->response_body + stream_data->response_pos, data, len);
    }
    stream_data->response_pos += len;
  }
  return 0;
}

/**
 * nghttp2_on_stream_close_callback: Called when a stream is about to be closed.
 */
int Http2Session::on_stream_close_callback(nghttp2_session *session, int32_t stream_id,
                                           uint32_t error_code, void *user_data) {
  auto *session_data = static_cast<Http2Session *>(user_data);
  ASSERT_ON_EVENT_LOOP(session_data);
  if (!session_data->HasStream(stream_id)) {
    return 0;
  }
  Http2Stream *stream_data =
      static_cast<Http2Stream *>(nghttp2_session_get_stream_user_data(session, stream_id));
  if (stream_data != nullptr) {
    const char *error_string =
        error_code <= 0 ? nghttp2_strerror(error_code) : nghttp2_http2_strerror(error_code);
    DEBUG_LOG("stream_id=%d closed with error_code=%u %s", stream_id, error_code, error_string);
    DEBUG_LOG("STATUS=%d", stream_data->status);
    if (error_code == 0) {
      if (stream_data->response_body) {
        stream_data->response_body[stream_data->response_pos] = 0;
      }
      ResponseMessage response_message = HttpResponseHandler::HandleResponse(
          stream_data->status, stream_data->response_headers["content-type"],
          stream_data->response_body, stream_data->response_pos, session_data->authority(),
          stream_data->bulk_request);
      // RECEIVE_RESPONSE
      Utils::PutTimestamp(stream_data->timestamps);
      stream_data->caller_cb(response_message, stream_data->caller_cb_args);
    } else {
      std::stringstream msg;
      msg << "HTTP2 stream closed with error (" << error_code << ") " << error_string;
      payload_t reply =
          session_data->object_mapper_->WriteValue<ErrorResponse>(ErrorResponse(msg.str()));
      OpStatus status = error_code == NGHTTP2_CANCEL ? OpStatus::OPERATION_CANCELLED
                                                     : OpStatus::SERVER_CHANNEL_ERROR;
      auto response_message = ResponseMessage(status, session_data->authority(), reply);
      // RECEIVE_RESPONSE
      Utils::PutTimestamp(stream_data->timestamps);
      stream_data->caller_cb(response_message, stream_data->caller_cb_args);
    }
    session_data->DetachStream(stream_id);
    if (stream_data->timeout_handler != nullptr) {
      stream_data->timeout_handler->Stop();
    }
    delete stream_data;
  }
  return 0;
}

void Http2Session::readcb_connect(struct bufferevent *bev, void *ptr) {
#if defined(DEBUG_LOG_ENABLED)
  auto *session_data = static_cast<Http2Session *>(ptr);
  DEBUG_LOG("readcb_connect session_data=%p", session_data);
  struct evbuffer *input = bufferevent_get_input(bev);
  size_t datalen = evbuffer_get_length(input);
  DEBUG_LOG("readcb_connect: data size=%ld", datalen);
#endif
}

void Http2Session::readcb(struct bufferevent *bev, void *ptr) {
  auto *session_data = static_cast<Http2Session *>(ptr);
  DEBUG_LOG("readcb session_data=%p", session_data);
  ssize_t readlen;
  struct evbuffer *input = bufferevent_get_input(bev);
  size_t datalen = evbuffer_get_length(input);
  unsigned char *data = evbuffer_pullup(input, -1);

  readlen = nghttp2_session_mem_recv(session_data->session_, data, datalen);
  if (readlen < 0) {
    warnx("Fatal error: (%ld) %s", readlen, nghttp2_strerror(readlen));
    delete session_data;
    return;
  }
  if (evbuffer_drain(input, static_cast<size_t>(readlen)) != 0) {
    warnx("Fatal error: evbuffer_drain failed");
    delete session_data;
    return;
  }
  int rv = nghttp2_session_send(session_data->session_);
  if (rv != 0) {
    warnx("Fatal error: (%d) %s", rv, nghttp2_strerror(rv));
    delete session_data;
    return;
  }
}

void Http2Session::writecb_connect(struct bufferevent *bev, void *ptr) {
  DEBUG_LOG("writecb_connect session_data=%p", reinterpret_cast<Http2Session *>(ptr));
}

/* writecb for bufferevent. To greaceful shutdown after sending or
   receiving GOAWAY, we check the some conditions on the nghttp2
   library and output buffer of bufferevent. If it indicates we have
   no business to this session, tear down the connection. */
void Http2Session::writecb(struct bufferevent *bev, void *ptr) {
  Http2Session *session_data = static_cast<Http2Session *>(ptr);
  DEBUG_LOG("writecb session_data=%p", session_data);

  if (nghttp2_session_want_read(session_data->session_) == 0 &&
      nghttp2_session_want_write(session_data->session_) == 0 &&
      evbuffer_get_length(bufferevent_get_output(session_data->bev_)) == 0) {
    delete session_data;  // KOKO
  }
}

void Http2Session::send_client_connection_header(Http2Session *session_data) {
  nghttp2_settings_entry iv[1] = {{NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, 100}};
  int rv;

  /* client 24 bytes magic string will be sent by nghttp2 library */
  rv = nghttp2_submit_settings(session_data->session_, NGHTTP2_FLAG_NONE, iv, ARRLEN(size_t, iv));
  if (rv != 0) {
    errx(1, "Could not submit SETTINGS: (%d) %s", rv, nghttp2_strerror(rv));
  }
}

void Http2Session::CompleteReconnection(const ResponseMessage &response_message, void *args) {
  auto *new_session_data = reinterpret_cast<Http2Session *>(args);
  if (response_message.status() == OpStatus::OK) {
    DEBUG_LOG("reconnection was successful pipe=%p session_data=%p pipe->session=%p",
              new_session_data->pipe_, new_session_data, new_session_data->pipe_->session_data());
  } else {
    DEBUG_LOG("reconnection failed for %s", (const char *)response_message.response_body());
  }
  (void)new_session_data;
  Utils::ReleasePayload(response_message.response_body());
}

void Http2Session::HandleConnectionFailure(Http2Session *session_data, const std::string &error,
                                           bool is_temporary) {
  DEBUG_LOG("Http2Session::HandleConnectionFailure session_data=%p reconn_delay=%ld,"
            " error=%s, is_temporary=%d", session_data, session_data->reconnection_delay_millis_,
            error.c_str(), is_temporary);
  auto pipe = session_data->pipe_;
  pipe->SetConnectionError(error);
  if (session_data->conn_cb_) {
    payload_t reply = session_data->object_mapper_->WriteValue<ErrorResponse>(ErrorResponse(error));
    session_data->conn_cb_(
        ResponseMessage(OpStatus::SERVER_CONNECTION_FAILURE, session_data->authority(), reply),
        session_data->conn_cb_args_);
  }
  if (is_temporary) {
    auto new_session_data = new Http2Session(pipe, pipe->host(), pipe->port(), pipe->cafile(),
                                             pipe->event_loop(), pipe->object_mapper());
    new_session_data->SetReconnection(session_data);
    DEBUG_LOG("Created a new session for reconnection: %p delay=%ld", new_session_data,
              new_session_data->reconnection_delay_millis_);

    InitiateConnectionParams *params =
        new InitiateConnectionParams(CompleteReconnection, new_session_data, new_session_data);
    if (new_session_data->reconnection_delay_millis_ == 0) {
      new_session_data->event_loop_->PutTask(InitiateConnection, params);
    } else {
      new_session_data->event_loop_->ScheduleTask(InitiateConnection, params,
                                                  new_session_data->reconnection_delay_millis_);
    }
  }
  DEBUG_LOG("Shutting down the session for failed connection: %p", session_data);
  session_data->Shutdown(NGHTTP2_CONNECT_ERROR);
}

/* eventcb for bufferevent. For the purpose of simplicity and
   readability of the example program, we omitted the certificate and
   peer verification. After SSL/TLS handshake is over, initialize
   nghttp2 library session, and send client connection header. Then
   send HTTP request. */
void Http2Session::eventcb_connect(struct bufferevent *bev, int16_t events, void *ptr) {
  auto *session_data = reinterpret_cast<Http2Session *>(ptr);
  DEBUG_LOG("eventcb_connect session_data=%p", session_data);
  if (events & BEV_EVENT_CONNECTED) {
    int fd = bufferevent_getfd(bev);

    SSL *ssl = bufferevent_openssl_get_ssl(session_data->bev_);

    X509 *peer_certificate = SSL_get_peer_certificate(ssl);
    if (peer_certificate == nullptr) {
      std::stringstream ss;
      ss << "A server certificate was not presented during the negotiation; "
            "host="
         << session_data->host_;
      HandleConnectionFailure(session_data, ss.str(), true);
      return;
    }

    // check server certificate verification result
    int64_t verify_result = SSL_get_verify_result(ssl);
    DEBUG_LOG("verify result: %s (%ld)", X509_verify_cert_error_string(verify_result),
              verify_result);
    if (verify_result != X509_V_OK) {
      std::stringstream ss;
      ss << "Failed to verify server certificate: " << X509_verify_cert_error_string(verify_result)
         << " (" << std::to_string(verify_result) << ")";
      HandleConnectionFailure(session_data, ss.str(), true);
      X509_free(peer_certificate);
      return;
    }

    /*
       Our certs support wildcards in hostnames => *.someclient.isima.io should validate the
       following hostnames:
          - someclient.isima.io
          - test.someclient.isima.io

       but, not --> abc.test.someclient.isima.io

       The wildcards are allowed for a single subdomain only
       Refer https://datatracker.ietf.org/doc/html/rfc6125 for details
    */
    if (X509_check_host(peer_certificate, session_data->host_.c_str(),
        static_cast<int>(session_data->host_.size()), 0, NULL) != 1) {
      const int kNameLength = 256;
      char peer_CN[kNameLength];
      X509_NAME_get_text_by_NID(X509_get_subject_name(peer_certificate), NID_commonName, peer_CN,
                            kNameLength);
      X509_free(peer_certificate);
      std::stringstream ss;
      ss << "Common name does not match host name; common=" << peer_CN
      << ", host=" << session_data->host_;
      HandleConnectionFailure(session_data, ss.str(), true);
      return;
    }

    X509_free(peer_certificate);

    unsigned int alpnlen = 0;
    const unsigned char *alpn = nullptr;
    SSL_get0_alpn_selected(ssl, &alpn, &alpnlen);

    if (alpn == nullptr || alpnlen != 2 || memcmp("h2", alpn, 2) != 0) {
      std::string error = "h2 is not negotiated";
      DEBUG_LOG("%s", error.c_str());
      HandleConnectionFailure(session_data, error, false);
      return;
    }

    // switch the callback function
    bufferevent_setcb(bev, readcb, writecb, eventcb, session_data);

    const int optval = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &optval, sizeof(optval));
    setsockopt(fd, IPPROTO_TCP, SO_KEEPALIVE, &optval, sizeof(optval));
    session_data->SetHttp2Callbacks();
    send_client_connection_header(session_data);
    int rv = nghttp2_session_send(session_data->session_);
    if (rv != 0) {
      warnx("Fatal error: (%d) %s", rv, nghttp2_strerror(rv));
      std::string error = "HTTP2 session initiation failed: (";
      error += std::to_string(rv);
      error += ") ";
      error += nghttp2_strerror(rv);
      HandleConnectionFailure(session_data, error, false);
    } else {
      session_data->pipe_->AttachSession(session_data);
      session_data->pipe_->ClearConnectionError();
      session_data->conn_cb_(ResponseMessage(OpStatus::OK, session_data->authority(),
                                             {
                                                 .data = nullptr,
                                                 .length = 0,
                                             }),
                             session_data->conn_cb_args_);
    }
    // TODO(Mayur): _clientMetrics ConnectionEstablished
    return;
  }
  std::string error;
  if (events & BEV_EVENT_EOF) {
    error = "Disconnected from the remote host";
  } else if (events & BEV_EVENT_ERROR) {
    error = "Server connection failure";
  } else if (events & BEV_EVENT_TIMEOUT) {
    error = "Server connection timed out";
  }
  HandleConnectionFailure(session_data, error, true);
}

/* eventcb invoked by bufferevent. registered after the connection has been
 * established. */
void Http2Session::eventcb(struct bufferevent *bev, int16_t events, void *ptr) {
  auto *session_data = reinterpret_cast<Http2Session *>(ptr);
  if (session_data->terminated_) {
    DEBUG_LOG(
        "Http2Session::eventcb -- ignoring events %d; session has been "
        "termianted already",
        events);
    return;
  }
  if (events & BEV_EVENT_TIMEOUT) {
    return;
  }
  std::string error;
  if (events & BEV_EVENT_EOF) {
    error = "Disconnected from the remote host";
  } else if (events & BEV_EVENT_ERROR) {
    error = "Network error";
  }
  DEBUG_LOG("Sock Event: %s", error.c_str());
  session_data->HandleConnectionError(NGHTTP2_CONNECT_ERROR);
}

}  // namespace tfos::csdk
