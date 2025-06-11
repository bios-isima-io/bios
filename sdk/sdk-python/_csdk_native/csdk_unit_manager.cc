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
#include "csdk_unit_manager.h"

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <sys/types.h>
#include <unistd.h>

#include <unordered_map>

/**
 * C-SDK unit manager constructor.
 */
CSdkUnitManager::CSdkUnitManager(capi_t capi, CSdkUnitManager *old) : capi_(capi), old_(old) {
  pid_ = getpid();
  sessions_ = new std::unordered_map<int, SessionProperties *>();
  bulk_ctx_map_ = new std::unordered_map<int, CallContext *>();
  if (old != nullptr) {
    old->new_ = this;
  }
}

CSdkUnitManager::~CSdkUnitManager() {
  if (sessions_) {
    delete sessions_;
    sessions_ = nullptr;
  }
  if (bulk_ctx_map_) {
    delete bulk_ctx_map_;
    bulk_ctx_map_ = nullptr;
  }
  capi_ = 0;
}

void CSdkUnitManager::Terminate() {
  if (new_ != nullptr) {
    new_->old_ = old_;
  }
  if (old_ != nullptr) {
    old_->new_ = new_;
  }
  delete this;
}

/**
 * Add a session properties object.
 */
SessionProperties *CSdkUnitManager::RegisterSessionProps(int session_handle, int fdw) {
  if (!IsValid()) {
    char message[512];
    snprintf(message, sizeof(message),
             "This C-SDK unit has been forked from process %d to subprcoess %d."
             " A forked unit is not usable.",
             pid_, getpid());
    PyErr_SetString(PyExc_RuntimeError, message);
    return nullptr;
  }

  auto it = sessions_->find(session_handle);
  SessionProperties *props;
  if (it == sessions_->end()) {
    props = new SessionProperties(fdw, capi_, this);
    (*sessions_)[session_handle] = props;
    return props;
  } else {
    PyErr_SetString(PyExc_RuntimeError, "Session handle is already in use.");
    return nullptr;
  }
}

/**
 * Find a session properties object by a session handle.
 * If the properties are not found, the method sets PyErr and returns nullptr.
 *
 * @param session_handle Handle of the session to look for
 * @param skip_validity_check Flag to skip checking the C-SDK unit validity
 */
SessionProperties *CSdkUnitManager::GetSessionProps(int session_handle, bool skip_validity_check) {
  auto it = sessions_->find(session_handle);
  if (it == sessions_->end()) {
    if (old_ != nullptr) {
      return old_->GetSessionProps(session_handle, skip_validity_check);
    }
    PyErr_SetString(PyExc_RuntimeError, "Session handle not found. Start the session first.");
    return nullptr;
  }

  auto props = it->second;
  if (props->closed) {
    PyErr_SetString(PyExc_RuntimeError, "Session is closed already");
    return nullptr;
  }

  if (IsValid() || skip_validity_check) {
    return props;
  }
  char message[512];
  snprintf(message, sizeof(message),
           "The session object has been forked from process %d to subprcoess %d."
           " A forked object is not usable.",
           pid_, getpid());
  PyErr_SetString(PyExc_RuntimeError, message);
  return nullptr;
}

/**
 * Remove Session Properties
 */
void CSdkUnitManager::RemoveSessionProps(int session_handle) {
  auto it = sessions_->find(session_handle);
  if (it == sessions_->end()) {
    if (old_ != nullptr) {
      old_->RemoveSessionProps(session_handle);
    }
    return;
  }

  auto props = it->second;
  sessions_->erase(it);

  // Sessin properties may not be ready to be deleted at this point of time when
  // there are pending operations. We check the prop's reference counter and
  // wait until pending operations are done. But if it takes too long, the
  // method gives up deleting the props and just leave. It would cause memory
  // leak, but it's usually a process termination by user interruption when a
  // session closes while operations are still running.
  useconds_t usec = 100;
  useconds_t total = 0;
  bool notified = false;
  const useconds_t kOneSecond = 1000000;
  const useconds_t kThreeSeconds = 3000000;
  const useconds_t kSixSeconds = 6000000;
  while (props->refcount.load() > 0) {
    DEBUG_LOG("refcount=%d still not ready to delete props", props->refcount.load());
    // Py_BEGIN_ALLOW_THREADS;  // Release GIL
    usleep(usec);
    total += usec;
    if (usec < kOneSecond) {
      usec *= 2;
    }
    // Py_END_ALLOW_THREADS;  // Obtain GIL

    if (total > kThreeSeconds && !notified) {  // 3 secs
      fprintf(stderr, "Waiting for pending operation to complete\n");
      notified = true;
    }
    if (total > kSixSeconds) {
      // taking too long. better to leak than to freeze
      break;
    }
  }
  if (props->refcount.load() == 0) {
    DEBUG_LOG("refcount=%d ok ready to delete", props->refcount.load());
    delete props;
#ifdef DEBUG_LOG_ENABLED
  } else {
    DEBUG_LOG("refcount=%d still pending but leave without deleting (would leak)",
              props->refcount.load());
#endif
  }

  // Terminate this
  if (!IsValid() && sessions_->empty()) {
    // TODO: destroy the unit
  }
}

/**
 * Add a bulk ingest context object.
 */
CallContext *CSdkUnitManager::RegisterIngestBulkContext(int seqno, int session_handle,
                                                        CSdkOperationId op_id) {
  SessionProperties *props = GetSessionProps(session_handle);
  if (props == nullptr) {
    PyErr_SetString(PyExc_RuntimeError, "Session handle not found. Start the session first.");
    return nullptr;
  }

  auto it = bulk_ctx_map_->find(seqno);
  if (it == bulk_ctx_map_->end()) {
    CallContext *context =
        new CallContext(seqno, props->fdw, props->responses, op_id, props->ask_q, &props->refcount);
    (*bulk_ctx_map_)[seqno] = context;
    return context;
  } else {
    PyErr_SetString(PyExc_RuntimeError, "Ingest bulk for this sequence already in progress");
    return nullptr;
  }
}

/**
 * Find a bulk context object given bulk context
 */
CallContext *CSdkUnitManager::GetIngestBulkContext(int seqno, int session_handle) {
  SessionProperties *props = GetSessionProps(session_handle);
  if (props == nullptr) {
    PyErr_SetString(PyExc_RuntimeError, "Session handle not found. Start the session first.");
    return nullptr;
  }
  auto it = bulk_ctx_map_->find(seqno);
  if (it == bulk_ctx_map_->end()) {
    PyErr_SetString(PyExc_RuntimeError, "Ingest bulk not in progress anymore");
    return nullptr;
  } else {
    return it->second;
  }
}

/**
 * Remove bulk ingest context when ingest bulk ends
 */
void CSdkUnitManager::RemoveIngestBulkContext(int seqno) {
  auto it = bulk_ctx_map_->find(seqno);
  if (it != bulk_ctx_map_->end()) {
    auto ctx = it->second;
    bulk_ctx_map_->erase(it);
    delete ctx;
  }
}
