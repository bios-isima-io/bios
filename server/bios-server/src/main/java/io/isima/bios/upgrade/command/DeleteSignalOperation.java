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
package io.isima.bios.upgrade.command;

import io.isima.bios.exceptions.ApplicationException;

/** Deletes a given signal from the specified tenant. */
public class DeleteSignalOperation implements UpgradeOperation {
  private final String signalName;
  private final String tenantName;
  private final UpgradeAdminController controller;

  DeleteSignalOperation(String tenantName, String signalName, UpgradeAdminController controller) {
    this.signalName = signalName;
    this.tenantName = tenantName;
    this.controller = controller;
  }

  @Override
  public void execute() throws ApplicationException {
    this.controller.deleteSignal(tenantName, signalName);
  }
}
