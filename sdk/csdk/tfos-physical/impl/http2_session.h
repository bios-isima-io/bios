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
#ifndef TFOS_PHYSICAL_HTTP2_SESSION_H_
#define TFOS_PHYSICAL_HTTP2_SESSION_H_

#include <event2/bufferevent_ssl.h>
#include <event2/event.h>
#include <nghttp2/nghttp2.h>
#include <openssl/ssl.h>

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "tfoscsdk/log.h"
#include "tfoscsdk/message_struct.h"
#include "tfos-common/utils.h"

namespace tfos::csdk {

#ifndef DEBUG_LOG_ENABLED
#define PRINT_HEADER(name, namelen, value, valuelen)
#define PRINT_HEADERS(nvas)
#else
#define PRINT_HEADER(name, namelen, value, valuelen) \
  Http2Session::PrintHeader(stderr, name, namelen, value, valuelen)
#define PRINT_HEADERS(nvas) Http2Session::PrintHeaders(stderr, nvas)

#endif

class EventLoop;
class ObjectMapper;
class PhysicalPipeImpl;

// Http2Stream
// ///////////////////////////////////////////////////////////////////////////////////
class Http2Session;
class Http2Stream;

/**
 * Class to handle timeout.
 *
 * An instance of this class borrows the event loop from the Http2Session and
 * initializes a timer event. The handler is attached to each stream and starts
 * after the request headers are sent successfully. The timer callback function
 * reports the caller timeout error and cancels the stream.
 */
class TimeoutHandler final {
 private:
  Http2Session *session_data_;
  Http2Stream *stream_data_;
  int32_t stream_id_;
  struct event_base *event_base_;
  struct event *ev_;
  int64_t timeout_millis_;
  struct timeval delay_;

 public:
  /**
   * The constructor initializes the timeout handler, but the timer does not
   * start.
   */
  TimeoutHandler(Http2Stream *stream_data, int64_t timeout_millis);

  /**
   * The destructor.
   */
  ~TimeoutHandler();

  /**
   * Starts the timer.
   */
  void Start();

  /**
   * Cancels the timer.
   */
  void Stop();

 private:
  TimeoutHandler() {}  // default constructor is disabled.

  /**
   * The timer callback function.
   */
  static void TimeoutCallback(evutil_socket_t fd, int16_t what, void *args);
};

struct Http2Stream {
  Http2Session *const session_data;
  std::string method;
  std::string path;
  std::string content_type;
  const ssize_t request_size;
  std::string request_size_string;
  nghttp2_data_provider provider;
  nghttp2_data_provider *p_provider;
  std::vector<nghttp2_nv> headers;
  response_cb const caller_cb;
  void *const caller_cb_args;
  std::string client_version;

  /* The stream ID of this stream */
  int32_t stream_id;

  // flow control
  ssize_t data_position;

  // response metadata
  int status;
  std::unordered_map<std::string, std::string> response_headers;
  int32_t error_code;  // error code in case of aborting the stream

  // timeout control
  int64_t timeout_millis;
  TimeoutHandler *timeout_handler;

  // response content
  uint8_t *response_body;
  size_t response_capacity;
  int response_pos;

  // metrics
  payload_t *timestamps;

  bool bulk_request;

  Http2Stream(Http2Session *session_data, const std::string &method, const std::string &path,
              const std::string &content_type, ssize_t request_size, int64_t timeout_millis,
              response_cb cb, void *cb_args, bool bulk)
      : session_data(session_data),
        method(method),
        content_type(content_type),
        request_size(request_size),
        provider({0}),
        p_provider(nullptr),
        caller_cb(cb),
        caller_cb_args(cb_args),
        error_code(NGHTTP2_NO_ERROR),
        timeout_millis(timeout_millis),
        timeout_handler(nullptr),
        timestamps(nullptr),
        bulk_request(bulk) {
    client_version = Utils::GetVersion();
    // configuration
    stream_id = -1;
    this->path = path.empty() ? "/" : path;
    request_size_string = std::to_string(request_size);

    // initialize flow control parameters
    data_position = 0;

    // initialize response parameters
    status = -1;
    response_body = nullptr;
    response_capacity = 0;
    response_pos = 0;
  }

  ~Http2Stream() { delete timeout_handler; }
};

// Http2Session
// //////////////////////////////////////////////////////////////////////////////////

class Http2Session final {
 private:
  // corresponding PhysicalPipeImpl object.
  PhysicalPipeImpl *pipe_;
  // Associated nghttp2 session object
  nghttp2_session *session_;

  // connectivity info
  std::string host_;
  uint16_t port_;
  std::string authority_;
  std::string cafile_;

  // Stream IDs that are running currently. Used for handling session events
  // that affect ongoing streams.
  std::unordered_set<int32_t> stream_ids_;

  // connection control
  SSL_CTX *ssl_ctx_;
  struct bufferevent *bev_;
  bool terminated_;          // flagged  when the session is terminated.
  int32_t shutdown_reason_;  // error code to return for operations cancelled by
                             // shutdown
  int connection_count_;
  uint64_t reconnection_delay_millis_;
  EventLoop *const event_loop_;

  // external utilities
  const ObjectMapper *const object_mapper_;

  // used temporarilly to control connection
  response_cb conn_cb_;
  void *conn_cb_args_;

  // for debugging
  uint32_t magic_;

 public:
  /**
   * Global initialization method that should be called before making an
   * instance of this class.
   */
  static void GlobalInit();

  Http2Session(PhysicalPipeImpl *pipe, const std::string &host, uint16_t port,
               const std::string &cafile, EventLoop *event_loop, const ObjectMapper *object_mapper);

  // getters and setters /////////////////////////////////////////
  inline nghttp2_session *session() const { return session_; }
  inline const std::string &authority() const { return authority_; }
  inline const ObjectMapper *object_mapper() const { return object_mapper_; }
  inline bool terminated() const { return terminated_; }
  inline EventLoop *event_loop() { return event_loop_; }
  inline PhysicalPipeImpl *pipe() const { return pipe_; }

  /**
   * Shutdown the session.
   *
   * @param error_code Nghttp2 error code that indicates the reason to shutdown
   */
  void Shutdown(uint32_t error_code);

  static void RunShutdown(void *args);

  void Discard(uint64_t delay_millis);

  static void RunDiscard(void *args);

  void HandleConnectionError(uint32_t reason);  // RFC 7540 section 5.4.1 TODO put doc

  void DetachPipe();

  void AttachStream(int32_t stream_id);

  void DetachStream(int32_t stream_id);

  bool HasStream(int32_t stream_id);

  /**
   * Check whether there are any pending streams.
   */
  bool HasPendingStreams();

  /**
   * Returns whether the session is ready.
   */
  bool IsReady();

  struct InitiateConnectionParams {
    response_cb cb;
    void *cb_args;
    Http2Session *session_data;

    InitiateConnectionParams(response_cb cb, void *cb_args, Http2Session *session_data)
        : cb(cb), cb_args(cb_args), session_data(session_data) {}
  };

  /**
   * Initiates a SSL connection to the endpoint and start an nghttp2 session.
   * @param cb Connection callback
   */
  static void InitiateConnection(void *args);

  void InitiateReconnection();

  /**
   * Retrieve reconnection parameters of the previous failed session and set new
   * parameters for this session.
   */
  void SetReconnection(Http2Session *previous);

  /**
   * Tells the event base to start the connection task.
   */
  static void InvokeSession(void *args);

  void AbortStream(OpStatus status, const std::string &message, int rcv_error, int32_t send_error,
                   Http2Stream *stream_data);

  void ReportError(OpStatus status, const std::string &error_message, Http2Stream *stream_data);

  /**
   * Send a RST_STREAM frame to the peer, detach the stream from the session,
   * and delete the stream.
   */
  static void RunAbortStream(void *args);

  static void PrintHeader(FILE *f, const uint8_t *name, size_t namelen, const uint8_t *value,
                          size_t valuelen);

  /* Print HTTP headers to |f|. Please note that this function does not
     take into account that header name and value are sequence of
     octets, therefore they may contain non-printable characters. */
  static void PrintHeaders(FILE *f, const std::vector<nghttp2_nv> &nvas);

 private:
  ~Http2Session();

  void SetHttp2Callbacks();

  /* nghttp2_send_callback. Here we transmit the |data|, |length| bytes,
     to the network. Because we are using libevent bufferevent, we just
     write those bytes into bufferevent buffer. */
  static ssize_t send_callback(nghttp2_session *session, const uint8_t *data, size_t length,
                               int flags, void *user_data);

  /* Callback function invoked when :enum:`NGHTTP2_DATA_FLAG_NO_COPY` is
     used in :type:`nghttp2_data_source_read_callback` to send complete
     DATA frame. */
  static int send_data_callback(nghttp2_session *session, nghttp2_frame *frame,
                                const uint8_t *framehd, size_t length, nghttp2_data_source *source,
                                void *user_data);

  /* nghttp2_on_header_callback: Called when nghttp2 library emits
     single header name/value pair. */
  static int on_header_callback(nghttp2_session *session, const nghttp2_frame *frame,
                                const uint8_t *name, size_t namelen, const uint8_t *value,
                                size_t valuelen, uint8_t flags, void *user_data);

  /* nghttp2_on_begin_headers_callback: Called when nghttp2 library gets
     started to receive header block. */
  static int on_begin_headers_callback(nghttp2_session *session, const nghttp2_frame *frame,
                                       void *user_data);

  /* nghttp2_on_frame_recv_callback: Called when nghttp2 library
     received a complete frame from the remote peer. */
  static int on_frame_recv_callback(nghttp2_session *session, const nghttp2_frame *frame,
                                    void *user_data);

  /* nghttp2_on_data_chunk_recv_callback: Called when DATA frame is
     received from the remote peer. In this implementation, if the frame
     is meant to the stream we initiated, print the received data in
     stdout, so that the user can redirect its output to the file
     easily. */
  static int on_data_chunk_recv_callback(nghttp2_session *session, uint8_t flags, int32_t stream_id,
                                         const uint8_t *data, size_t len, void *user_data);

  /**
   * nghttp2_on_stream_close_callback: Called when a stream is about to
   * closed.
   */
  static int on_stream_close_callback(nghttp2_session *session, int32_t stream_id,
                                      uint32_t error_code, void *user_data);

  static void readcb_connect(struct bufferevent *bev, void *ptr);

  static void readcb(struct bufferevent *bev, void *ptr);

  static void writecb_connect(struct bufferevent *bev, void *ptr);

  /* writecb for bufferevent. To greaceful shutdown after sending or
     receiving GOAWAY, we check the some conditions on the nghttp2
     library and output buffer of bufferevent. If it indicates we have
     no business to this session, tear down the connection. */
  static void writecb(struct bufferevent *bev, void *ptr);

  static void send_client_connection_header(Http2Session *session_data);

  static void CompleteReconnection(const ResponseMessage &response_message, void *args);

  static void HandleConnectionFailure(Http2Session *session_data, const std::string &error,
                                      bool is_temporary);

  /* eventcb for bufferevent. For the purpose of simplicity and
     readability of the example program, we omitted the certificate and
     peer verification. After SSL/TLS handshake is over, initialize
     nghttp2 library session, and send client connection header. Then
     send HTTP request. */
  static void eventcb_connect(struct bufferevent *bev, int16_t events, void *ptr);

  /* eventcb invoked by bufferevent. registered after the connection has been
   * established. */
  static void eventcb(struct bufferevent *bev, int16_t events, void *ptr);
};

}  // namespace tfos::csdk
#endif  // TFOS_PHYSICAL_PHYSICAL_PIPE_H_
