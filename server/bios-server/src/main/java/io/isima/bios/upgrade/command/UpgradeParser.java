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
import io.isima.bios.models.BiosVersion;
import io.isima.bios.upgrade.models.UpgradeVersionConfig;
import java.util.ArrayList;
import java.util.List;

/**
 * Extracts commands in the upgrade config from current version to target version, where current
 * version is the current cluster version and target version is the version of bits that is being
 * upgraded.
 *
 * <p>Note: For the first upgrade, the current cluster version will be 0.0.0. This effectively means
 * all entries up to the target version will be applied by the upgrade process.
 */
public class UpgradeParser {
  private final BiosVersion currentVersion;
  private final BiosVersion targetVersion;
  private final UpgradeOperationFactory commandFactory;

  UpgradeParser(
      BiosVersion currentVersion,
      BiosVersion targetVersion,
      UpgradeOperationFactory commandFactory) {
    this.currentVersion = currentVersion;
    this.targetVersion = targetVersion;
    this.commandFactory = commandFactory;
  }

  public List<VersionUpgradeOperation> parse(List<UpgradeVersionConfig> upgradeConfig)
      throws ApplicationException {
    List<VersionUpgradeOperation> upgradeCommands = new ArrayList<>();
    for (final var u : upgradeConfig) {
      final var v = new BiosVersion(u.getTargetVersion());
      if (v.compareTo(currentVersion) > 0 && v.compareTo(targetVersion) <= 0) {
        upgradeCommands.add(createFromConfig(u, v));
      }
    }
    return upgradeCommands;
  }

  private VersionUpgradeOperation createFromConfig(
      UpgradeVersionConfig upgradeVersionConfig, BiosVersion targetVersion)
      throws ApplicationException {
    List<UpgradeOperation> parsedCommands = new ArrayList<>();
    for (final var cmd : upgradeVersionConfig.getUpgradeCommands()) {
      final String tenantName = cmd.getTenantName();
      for (final var cmdString : cmd.getCommands()) {
        parsedCommands.add(parseCommandString(tenantName, cmdString));
      }
    }
    return new VersionUpgradeOperation(parsedCommands, targetVersion);
  }

  private UpgradeOperation parseCommandString(String tenantName, String commandString)
      throws ApplicationException {
    // currently only a simple delete signal <signal_name> is supported
    // TODO(ramesh) : add more sophisticated parsing to support different commands
    String[] tokens = commandString.split("\\s+");
    if (tokens.length < 3) {
      throw new ApplicationException("Upgrade command wrongly specified: " + commandString);
    }
    if (tokens[0].equalsIgnoreCase("delete") && tokens[1].equalsIgnoreCase("signal")) {
      return commandFactory.createDeleteSignalCommand(tenantName, tokens[2]);
    }
    throw new ApplicationException("Upgrade command syntax not supported yet: " + commandString);
  }
}
