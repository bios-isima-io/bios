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

import io.isima.bios.common.BuildVersion;
import io.isima.bios.common.SharedProperties;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.models.BiosVersion;
import io.isima.bios.upgrade.common.UpgradeType;

public class UpgradeMetadataController {
  private static final String CURRENT_CLUSTER_VERSION_KEY = "currentClusterVersion";
  private static final String CLUSTER_UPGRADING_BY_KEY = "clusterUpgradingByNode";
  private static final String TARGET_CLUSTER_VERSION_KEY = "targetClusterVersion";

  // upgrade history format => <timestamp>:<action>:<prevVersion>:<currentVersion>:<upgradingNodeId>
  private static final String CLUSTER_VERSION_HISTORY_KEY = "clusterVersionHistory";

  private static final String NODE_ID_DELIMITER = ".";
  private static final String CURRENT_NODE_VERSION_KEY = "currentNodeVersion";

  // upgrade history format => <timestamp>:<prevVersion>:<currentVersion>
  private static final String NODE_VERSION_HISTORY_KEY = "nodeVersionHistory";

  private static final String FIELD_DELIMITER = ":";
  private static final String RECORD_DELIMITER = ";  ";
  private static final String START_CLUSTER_UPGRADE_ACTION = "Start Cluster Upgrade";
  private static final String CONTINUE_CLUSTER_UPGRADE_ACTION = "Continue Cluster Upgrade";
  private static final String END_CLUSTER_UPGRADE_ACTION = "End Cluster Upgrade";

  private final SharedProperties sharedProperties;
  private final String nodeVersionKey;
  private final String nodeVersionHistoryKey;
  private final BiosVersion targetUpgradeVersion;

  private final String thisNodeId;

  private BiosVersion currentClusterVersion;

  private BiosVersion currentNodeVersion;

  public UpgradeMetadataController(SharedProperties sharedProperties, String thisNodeId) {
    this.sharedProperties = sharedProperties;
    this.thisNodeId = thisNodeId;
    this.nodeVersionKey = thisNodeId + NODE_ID_DELIMITER + CURRENT_NODE_VERSION_KEY;
    this.nodeVersionHistoryKey = thisNodeId + NODE_ID_DELIMITER + NODE_VERSION_HISTORY_KEY;
    this.targetUpgradeVersion = new BiosVersion(BuildVersion.VERSION);
  }

  public UpgradeType computeUpgradeType() throws ApplicationException {
    currentClusterVersion =
        new BiosVersion(sharedProperties.getPropertyFailOnError(CURRENT_CLUSTER_VERSION_KEY));
    String clusterUpgradingByNode =
        sharedProperties.getPropertyFailOnError(CLUSTER_UPGRADING_BY_KEY);
    if (clusterUpgradingByNode == null) {
      clusterUpgradingByNode = "";
    }
    currentNodeVersion = new BiosVersion(sharedProperties.getPropertyFailOnError(nodeVersionKey));
    if (currentClusterVersion.equals(targetUpgradeVersion)) {
      if (currentNodeVersion.equals(targetUpgradeVersion)) {
        return UpgradeType.NO_UPGRADE;
      } else {
        return UpgradeType.NODE_UPGRADE;
      }
    } else {
      if (clusterUpgradingByNode.isEmpty() || clusterUpgradingByNode.equals(thisNodeId)) {
        return clusterUpgradingByNode.isEmpty()
            ? UpgradeType.FRESH_CLUSTER_UPGRADE
            : UpgradeType.CONTINUE_CLUSTER_UPGRADE;
      } else {
        // other node is trying to upgrade individual node while cluster upgrade is still
        // going on
        throw new ApplicationException(
            String.format(
                "Node %s cannot start as cluster is being upgraded by %s",
                thisNodeId, clusterUpgradingByNode));
      }
    }
  }

  String getThisNodeId() {
    return thisNodeId;
  }

  void stampClusterUpgradeStart(boolean continueUpgrade) throws ApplicationException {
    sharedProperties.setProperty(CLUSTER_UPGRADING_BY_KEY, thisNodeId);
    sharedProperties.setProperty(TARGET_CLUSTER_VERSION_KEY, targetUpgradeVersion.toString());

    String sb =
        System.currentTimeMillis()
            + FIELD_DELIMITER
            + ((continueUpgrade) ? CONTINUE_CLUSTER_UPGRADE_ACTION : START_CLUSTER_UPGRADE_ACTION)
            + FIELD_DELIMITER
            + currentClusterVersion
            + FIELD_DELIMITER
            + targetUpgradeVersion
            + FIELD_DELIMITER
            + thisNodeId;
    sharedProperties.appendPropertyIfAbsent(
        CLUSTER_VERSION_HISTORY_KEY, sb, RECORD_DELIMITER, true);
  }

  void stampClusterUpgradeEnd() throws ApplicationException {
    sharedProperties.setProperty(CLUSTER_UPGRADING_BY_KEY, "");
    sharedProperties.setProperty(CURRENT_CLUSTER_VERSION_KEY, targetUpgradeVersion.toString());
    String history =
        System.currentTimeMillis()
            + FIELD_DELIMITER
            + END_CLUSTER_UPGRADE_ACTION
            + FIELD_DELIMITER
            + currentClusterVersion
            + FIELD_DELIMITER
            + targetUpgradeVersion
            + FIELD_DELIMITER
            + thisNodeId;
    sharedProperties.appendPropertyIfAbsent(
        CLUSTER_VERSION_HISTORY_KEY, history, RECORD_DELIMITER, true);
    stampNodeUpgrade();
  }

  void stampNodeUpgrade() throws ApplicationException {
    sharedProperties.setProperty(nodeVersionKey, targetUpgradeVersion.toString());
    String nodeHistory =
        System.currentTimeMillis()
            + FIELD_DELIMITER
            + currentNodeVersion
            + FIELD_DELIMITER
            + targetUpgradeVersion;
    sharedProperties.appendPropertyIfAbsent(
        nodeVersionHistoryKey, nodeHistory, RECORD_DELIMITER, true);
  }

  BiosVersion getCurrentClusterVersion() {
    return currentClusterVersion;
  }
}
