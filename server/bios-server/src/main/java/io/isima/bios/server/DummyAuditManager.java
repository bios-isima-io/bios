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
package io.isima.bios.server;

import io.isima.bios.audit.AuditInfo;
import io.isima.bios.audit.IAuditManager;
import javax.ws.rs.core.Response;

public class DummyAuditManager implements IAuditManager {

  @Override
  public void commit(AuditInfo auditInfo, String status, String response) {
    // TODO: commit the audito record
  }

  @Override
  public void commit(AuditInfo auditInfo, Throwable e) {
    // TODO: commit the audito record
  }

  @Override
  public void commit(AuditInfo auditInfo, Response response) {
    // TODO: commit the audito record
  }
}
