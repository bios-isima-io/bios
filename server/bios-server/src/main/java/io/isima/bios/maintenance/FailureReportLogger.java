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

import io.isima.bios.common.BiosConstants;
import io.isima.bios.models.Event;
import io.isima.bios.utils.StringUtils;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Log helper class to bundle multiple failure report log messages to avoid the messages being
 * overwhelming.
 */
public class FailureReportLogger {
  private static final Logger logger = LoggerFactory.getLogger(FailureReportLogger.class);

  private static final long FAILURE_LOG_INTERVAL = 300000;
  private static final long FAILURE_LOG_COUNT = 500;

  private long failureReportLastLog;
  private long failureReportCount;
  private final Set<String> failureReportTargets;
  private final Set<String> failureReporters;
  private final Set<String> failedOperations;

  public FailureReportLogger() {
    failureReportCount = 0;
    failureReportLastLog = 0;
    failureReportTargets = new LinkedHashSet<>();
    failureReporters = new LinkedHashSet<>();
    failedOperations = new LinkedHashSet<>();
  }

  public void warn(String format, Object... arguments) {}

  public synchronized void reportFailure(Event error) {
    final long now = System.currentTimeMillis();
    if (failureReportLastLog == 0) {
      // the first one
      logger.warn("Received failure report; {}", error);
      failureReportLastLog = now;
      return;
    }

    ++failureReportCount;
    failureReportTargets.add(error.get(BiosConstants.ATTR_NODE_ID).toString());
    failureReporters.add(error.get(BiosConstants.ATTR_REPORTER).toString());
    failedOperations.add(error.get(BiosConstants.ATTR_OPERATION).toString());

    tryLoggingCore();
  }

  public synchronized void tryLogging() {
    tryLoggingCore();
  }

  private void tryLoggingCore() {
    final long now = System.currentTimeMillis();
    if (failureReportCount > 0
        && (failureReportCount >= FAILURE_LOG_COUNT
            || now - failureReportLastLog > FAILURE_LOG_INTERVAL)) {
      final var reporters = new ArrayList<>();
      final var failedOps = new ArrayList<>();
      int count = 0;
      for (var reporter : failureReporters) {
        reporters.add(reporter);
        if (++count == 5) {
          break;
        }
      }
      count = 0;
      for (var op : failedOperations) {
        failedOps.add(op);
        if (++count == 5) {
          break;
        }
      }

      logger.warn(
          "Received {} failure reports since {};"
              + " reportedNodes={}, reporters(up to 5)={}, failedOps(up to 5)={}",
          failureReportCount,
          StringUtils.tsToIso8601(failureReportLastLog),
          failureReportTargets,
          reporters,
          failedOps);
      failureReportTargets.clear();
      failureReporters.clear();
      failedOperations.clear();
      failureReportLastLog = now;
      failureReportCount = 0;
    }
  }
}
