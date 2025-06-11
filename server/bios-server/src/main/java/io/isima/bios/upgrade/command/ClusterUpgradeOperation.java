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

import io.isima.bios.errors.exception.FileReadException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.models.BiosVersion;
import io.isima.bios.upgrade.common.UpgradeConfigLoader;
import io.isima.bios.upgrade.models.UpgradeVersionConfig;
import java.util.List;

/**
 * Composes other commands specified in the upgrade file and executes them as a part of cluster
 * upgrade. This command also keeps track of meta data in a shared config using the metadata
 * controller.
 */
public class ClusterUpgradeOperation implements UpgradeOperation, UpgradeOperationFactory {
  private final UpgradeMetadataController metaController;
  private final UpgradeAdminController adminController;
  private final UpgradeParser upgradeParser;
  private final boolean continueUpgrade;

  public ClusterUpgradeOperation(
      UpgradeMetadataController upgradeConfig,
      UpgradeAdminController upgradeAdminController,
      BiosVersion targetVersion,
      boolean continueUpgrade) {
    this.metaController = upgradeConfig;
    this.adminController = upgradeAdminController;
    this.continueUpgrade = continueUpgrade;
    this.upgradeParser =
        new UpgradeParser(upgradeConfig.getCurrentClusterVersion(), targetVersion, this);
  }

  @Override
  public void execute() throws ApplicationException {
    metaController.stampClusterUpgradeStart(continueUpgrade);
    doClusterUpgrade();
    metaController.stampClusterUpgradeEnd();
  }

  @Override
  public DeleteSignalOperation createDeleteSignalCommand(String tenantName, String signalName) {
    return new DeleteSignalOperation(tenantName, signalName, adminController);
  }

  private void doClusterUpgrade() throws ApplicationException {
    List<UpgradeVersionConfig> upgradeConfig;
    try {
      upgradeConfig = UpgradeConfigLoader.load();
    } catch (FileReadException e) {
      throw new ApplicationException("Upgrade Failed: Unable to load upgrade configuration");
    }
    List<VersionUpgradeOperation> executors = upgradeParser.parse(upgradeConfig);
    for (final var cmd : executors) {
      cmd.execute();
    }
  }
}
