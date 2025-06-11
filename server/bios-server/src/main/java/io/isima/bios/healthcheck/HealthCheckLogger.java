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
package io.isima.bios.healthcheck;

import io.isima.bios.common.TfosConfig;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.maintenance.MaintenanceWorker;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HealthCheckLogger extends MaintenanceWorker {
  private static final Logger logger = LoggerFactory.getLogger(HealthCheckLogger.class);
  private final String application;
  private final Set<HealthCheckObserver> observers;

  public HealthCheckLogger(final String applicationName, BiosModules parentModules) {
    super(
        TfosConfig.healthCheckIntervalSeconds(),
        TfosConfig.healthCheckIntervalSeconds(),
        TimeUnit.SECONDS,
        null,
        parentModules);
    this.application = applicationName;
    this.observers = ConcurrentHashMap.newKeySet();
  }

  @Override
  public void runMaintenance() {
    final StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (final var observer : observers) {
      if (!first) {
        sb.append(":");
      }
      sb.append(observer.healthCheck());
      first = false;
    }
    String healthInfo = sb.toString();
    if (healthInfo.isEmpty()) {
      logger.info("Health Check Service : {} running fine", application);
    } else {
      logger.warn(
          "Health Check Service : {} not healthy. Health Check Details = {}",
          application,
          healthInfo);
    }
  }

  public void registerObserver(HealthCheckObserver observer) {
    observers.add(observer);
  }
}
