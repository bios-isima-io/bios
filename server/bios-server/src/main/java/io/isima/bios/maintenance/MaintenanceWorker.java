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
package io.isima.bios.maintenance;

import io.isima.bios.framework.BiosModules;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MaintenanceWorker implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(MaintenanceWorker.class);

  private final int initialDelay;
  private final int interval;
  private final TimeUnit timeUnit;
  protected final ServiceStatus serviceStatus;

  private final BiosModules parentModules;

  @Getter @Setter protected boolean isShutdown;

  public MaintenanceWorker(
      int initialDelay,
      int interval,
      TimeUnit timeUnit,
      ServiceStatus serviceStatus,
      BiosModules parentModules) {
    this.initialDelay = initialDelay;
    this.interval = interval;
    this.timeUnit = timeUnit;
    isShutdown = false;
    this.serviceStatus = serviceStatus;
    this.parentModules = parentModules;
  }

  public abstract void runMaintenance();

  @Override
  public void run() {
    try {
      logger.trace("{}: start a maintenance task", this.getClass().getSimpleName());
      runMaintenance();
    } catch (Throwable t) {
      logger.error(
          "MaintenanceWorker {} failed to execute the maintenance task; modules={}",
          this.getClass().getSimpleName(),
          parentModules.getModulesName(),
          t);
    }
  }

  /**
   * Method to get initial delay of worker execution.
   *
   * @return Initial delay
   */
  final int getInitialDelay() {
    return initialDelay;
  }

  /**
   * Method to get interval of worker execution.
   *
   * @return Interval
   */
  final int getInterval() {
    return interval;
  }

  /**
   * Method to get time unit of initial delay and interval values.
   *
   * @return The time unit
   */
  final TimeUnit getTimeUnit() {
    return timeUnit;
  }

  /** Method to be called when the service shuts down. */
  public void shutdown() {
    isShutdown = true;
  }
}
