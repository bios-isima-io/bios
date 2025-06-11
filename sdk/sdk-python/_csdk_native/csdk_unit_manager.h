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
#ifndef CSDK_UNIT_MANAGER_H_
#define CSDK_UNIT_MANAGER_H_

#include <unordered_map>

#include "py_common.h"

/**
 * C-SDK unit manager
 */
class CSdkUnitManager final {
 public:
  CSdkUnitManager(capi_t capi, CSdkUnitManager *old = nullptr);

  ~CSdkUnitManager();

  void Terminate();

  inline capi_t capi() const { return capi_; }
  inline pid_t pid() const { return pid_; }

  /**
   * Returns whether the C-SDK unit is valid by checking pid.
   * The C-SDK unit is broken if it's forked out to a subprocess.
   */
  bool IsValid() const { return getpid() == pid_; }

  size_t GetNumRegisteredSessions() { return sessions_->size(); }

  /**
   * Add a session properties object.
   */
  SessionProperties *RegisterSessionProps(int session_handle, int fdw);

  /**
   * Find a session properties object by a session handle.
   * If the properties are not found, the method sets PyErr and returns nullptr.
   *
   * @param session_handle Handle of the session to look for
   * @param skip_validity_check Flag to skip checking the C-SDK unit validity
   */
  SessionProperties *GetSessionProps(int session_handle, bool skip_validity_check = false);

  /**
   * Remove Session Properties
   */
  void RemoveSessionProps(int session_handle);

  /**
   * Add a bulk ingest context object.
   */
  CallContext *RegisterIngestBulkContext(int seqno, int session_handle, CSdkOperationId op_id);

  /**
   * Find a bulk context object given bulk context
   */
  CallContext *GetIngestBulkContext(int seqno, int session_handle);

  /**
   * Remove bulk ingest context when ingest bulk ends
   */
  void RemoveIngestBulkContext(int seqno);

  CSdkUnitManager *old() const { return old_; }

 private:
  capi_t capi_;
  // The pid is used to detect forking. The C-API unit is not usable after being
  // forked. We check current PID and the pid at the creation time, and obsolete
  // the unit if they are different.
  pid_t pid_;
  std::unordered_map<int, SessionProperties *> *sessions_;
  std::unordered_map<int, CallContext *> *bulk_ctx_map_;

  // A unit may be invalidated by forking. We keep old unit until it is ready to
  // be destroyed.
  CSdkUnitManager *old_;
  CSdkUnitManager *new_;
};

#endif  // CSDK_UNIT_MANAGER_H
