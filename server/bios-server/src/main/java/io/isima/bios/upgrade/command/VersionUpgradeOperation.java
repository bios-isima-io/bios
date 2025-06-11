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
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Upgrades to the next version. This continues up to the target version. */
public class VersionUpgradeOperation implements UpgradeOperation {
  private static final Logger logger = LoggerFactory.getLogger(VersionUpgradeOperation.class);

  private final List<UpgradeOperation> upgradeCommands;
  private final BiosVersion toVersion;

  VersionUpgradeOperation(List<UpgradeOperation> upgradeCommands, BiosVersion toVersion) {
    this.upgradeCommands = upgradeCommands;
    this.toVersion = toVersion;
  }

  @Override
  public void execute() throws ApplicationException {
    logger.info("Upgrading to version {}", toVersion);
    for (final var cmd : upgradeCommands) {
      cmd.execute();
    }
  }
}
