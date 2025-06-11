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
package io.isima.bios.upgrade;

import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.common.BuildVersion;
import io.isima.bios.common.SharedProperties;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.models.BiosVersion;
import io.isima.bios.upgrade.command.ClusterUpgradeOperation;
import io.isima.bios.upgrade.command.NodeUpgradeOperation;
import io.isima.bios.upgrade.command.UpgradeAdminController;
import io.isima.bios.upgrade.command.UpgradeMetadataController;
import io.isima.bios.upgrade.command.UpgradeOperation;
import io.isima.bios.upgrade.common.UpgradeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Upgrades the cluster from the current cluster version to the target version based on the target
 * version of the new bits.
 */
public class UpgradeSystem {
  private static final Logger logger = LoggerFactory.getLogger(UpgradeSystem.class);

  private final BiosVersion targetVersion;
  private final UpgradeMetadataController metaController;
  private final UpgradeAdminController adminController;

  public UpgradeSystem(SharedProperties sharedProperties, AdminInternal admin, String thisNodeId) {
    this.targetVersion = new BiosVersion(BuildVersion.VERSION);
    this.metaController = new UpgradeMetadataController(sharedProperties, thisNodeId);
    this.adminController = new UpgradeAdminController(admin);
  }

  public void upgrade() throws ApplicationException {
    UpgradeType upgradeType = metaController.computeUpgradeType();
    switch (upgradeType) {
      case NO_UPGRADE:
        break;

      case NODE_UPGRADE:
        UpgradeOperation nodeUpgrade = new NodeUpgradeOperation(metaController);
        nodeUpgrade.execute();
        break;

      case FRESH_CLUSTER_UPGRADE:
        UpgradeOperation freshUpgrade =
            new ClusterUpgradeOperation(metaController, adminController, targetVersion, false);
        freshUpgrade.execute();
        break;

      case CONTINUE_CLUSTER_UPGRADE:
        UpgradeOperation continueUpgrade =
            new ClusterUpgradeOperation(metaController, adminController, targetVersion, true);
        continueUpgrade.execute();
        break;

      default:
        logger.warn("Unable to determine upgrade");
        break;
    }
  }
}
