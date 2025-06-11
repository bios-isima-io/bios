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
#ifndef TFOSCSDK_PYCOMMON_H_
#define TFOSCSDK_PYCOMMON_H_

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>

#include <atomic>
#include <boost/lockfree/queue.hpp>
#include <unordered_map>

#include "tfoscsdk/directapi.h"
#include "tfoscsdk/version.h"

#define PROTOCOL_JSON ((uint8_t)0)
#define PROTOCOL_PROTOBUF ((uint8_t)1)

inline int64_t CurrentTimeMillis() {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

inline int64_t CurrentTimeMicros() {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return tv.tv_sec * 1000 * 1000 + tv.tv_usec;
}

/**
 * Struct that carries response data. The data is finally fetched by Python
 * wrapper via method FetchNext() to build operation results.
 */
// TODO(Naoki): It might be faster to stream this data into to the pipe fdw.
struct ResponseData final {
  int seqno;
  status_code_t status_code;
  payload_t response;
  string_t endpoint;
  uint8_t protocol_indicator;
  int32_t from_index;
  uint64_t start_time;
  uint64_t elapsed_time;
  uint64_t num_reads;
  uint64_t num_writes;
  uint64_t num_qos_retry_considered;
  uint64_t num_qos_retry_sent;
  uint64_t num_qos_retry_response_used;
};

/**
 * Struct that carries 'more' data for ingest bulk. This will trigger python
 * wrapper to call IngestBulk with appropriate parameters after a FetchNextBulk
 * call back to python native layer.
 */
struct NextIngestBulk final {
  int seqno;
  int from_index;
  int to_index;
  int64_t bulk_ctx_id;
};

class CSdkUnitManager;
/**
 * Struct that carries session properties.
 */
struct SessionProperties final {
  // Queue to exchange operation responses. The C-SDK enqueues the response data
  // on an operation completion followed by notification via fdw. Then the
  // Python wrapper fetches the data via method FetchNext.
  boost::lockfree::queue<ResponseData> *responses;

  // Queue to exchange 'more' data for IngestBulk
  boost::lockfree::queue<NextIngestBulk> *ask_q;

  // Pipe write FD. The Python wrapper is watching the corresponding pipe read
  // FD, then invokes its completion handler.
  int fdw;

  // C-SDK handle for the session
  capi_t capi;

  // Corresponding unit manager
  CSdkUnitManager *unit_manager;

  // Set when the session is closed
  volatile bool closed;

  // Reference count
  std::atomic_int refcount;

  // ctor and dtor
  SessionProperties(int fdw, capi_t capi, CSdkUnitManager *unit_manager)
      : responses(new boost::lockfree::queue<ResponseData>(1024)),
        ask_q(new boost::lockfree::queue<NextIngestBulk>(128)),
        fdw(fdw),
        capi(capi),
        unit_manager(unit_manager),
        closed(false),
        refcount(0) {}

  ~SessionProperties() {
    delete responses;
    delete ask_q;
  }
};

/**
 * Struct to carry a simple C-SDK operation call context.
 */
struct CallContext final {
  // Operation call sequence number. this is used for finding the corresponding
  // future object in the python layer.
  int seqno;

  // Path to return the response to the python layer.
  int fdw;
  boost::lockfree::queue<ResponseData> *responses;
  CSdkOperationId op_id;
  boost::lockfree::queue<NextIngestBulk> *ask_q;
  int32_t from_index;         // used by bulk ingest
  std::atomic_int *refcount;  // session properties reference count
  uint64_t start_time;
  uint64_t num_reads;
  uint64_t num_writes;

  CallContext(int seqno, int fdw, boost::lockfree::queue<ResponseData> *responses,
              std::atomic_int *refcount)
      : seqno(seqno),
        fdw(fdw),
        responses(responses),
        op_id(CSDK_OP_NOOP),
        ask_q(nullptr),
        from_index(0),
        refcount(refcount) {
    ++(*refcount);
    start_time = CurrentTimeMicros();
    UpdateNumRecords();
  }

  CallContext(int seqno, int fdw, boost::lockfree::queue<ResponseData> *responses,
              CSdkOperationId op_id, std::atomic_int *refcount)
      : seqno(seqno),
        fdw(fdw),
        responses(responses),
        op_id(op_id),
        ask_q(nullptr),
        from_index(0),
        refcount(refcount) {
    ++(*refcount);
    start_time = CurrentTimeMicros();
    UpdateNumRecords();
  }

  CallContext(int seqno, int fdw, boost::lockfree::queue<ResponseData> *responses,
              CSdkOperationId op_id, boost::lockfree::queue<NextIngestBulk> *ask_q,
              std::atomic_int *refcount)
      : seqno(seqno),
        fdw(fdw),
        responses(responses),
        op_id(op_id),
        ask_q(ask_q),
        from_index(0),
        refcount(refcount) {
    ++(*refcount);
    start_time = CurrentTimeMicros();
    UpdateNumRecords();
  }

  CallContext(const CallContext &src)
      : seqno(src.seqno),
        fdw(src.fdw),
        responses(src.responses),
        op_id(src.op_id),
        ask_q(src.ask_q),
        from_index(src.from_index),
        refcount(src.refcount) {
    ++(*refcount);
    start_time = CurrentTimeMicros();
    UpdateNumRecords();
  }

  ~CallContext() { --(*refcount); }

  void UpdateNumRecords() {
    switch (op_id) {
      case CSDK_OP_INSERT_PROTO:
      case CSDK_OP_CREATE_CONTEXT_ENTRY_BIOS:
      case CSDK_OP_UPDATE_CONTEXT_ENTRY_BIOS:
      case CSDK_OP_DELETE_CONTEXT_ENTRY_BIOS:
      case CSDK_OP_REPLACE_CONTEXT_ENTRY_BIOS:
        num_reads = 0;
        num_writes = 1;
        break;
      default:
        num_reads = 0;
        num_writes = 0;
        break;
    }
  }
};

#endif  // TFOSCSDK_PYCOMMON_H_
