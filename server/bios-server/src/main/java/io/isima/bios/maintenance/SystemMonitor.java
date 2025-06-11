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

import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.BiosConstants;
import io.isima.bios.common.EventFactory;
import io.isima.bios.common.ExtractState;
import io.isima.bios.data.DataEngine;
import io.isima.bios.data.storage.cassandra.CassandraConstants;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.ExecutorManager;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.healthcheck.HealthCheckObserver;
import io.isima.bios.models.Event;
import io.isima.bios.models.ExtractRequest;
import io.isima.bios.storage.cassandra.CassandraConnection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class to monitor the system status. */
public class SystemMonitor extends MaintenanceWorker implements HealthCheckObserver {
  private static final Logger logger = LoggerFactory.getLogger(SystemMonitor.class);

  // constants //////////
  private static final int INITIAL_DELAY = 0;
  private static final int WAKEUP_INTERVAL = 5;
  private static final int DRAIN_WAIT_BEFORE_RECOVERY_MILLIS = 5000;

  // components and parameters //////////
  private final ServiceStatus serviceStatus;
  private final AdminInternal admin;
  private final DataEngine dataEngine;
  private final ExecutorService asyncService;
  private final FailureReportLogger failureReportLogger;

  // states //////////
  private volatile boolean isShutdown;
  private long lastFailureReportCheckTime;

  /**
   * We count succession of recoverable errors from Cassandra. Such an exception is handled as
   * non-error, then the worker just back off and restart the task in the next rollup cycle, until
   * the count reaches to the pre-defined allowance. Beyond that count, the worker handles a
   * recoverable error as an error.
   */
  private int cassandraErrorCount;

  /** The constructor. */
  public SystemMonitor(
      ServiceStatus serviceStatus,
      AdminInternal admin,
      DataEngine dataEngine,
      BiosModules parentModules) {
    super(INITIAL_DELAY, WAKEUP_INTERVAL, TimeUnit.SECONDS, null, parentModules);

    this.serviceStatus = serviceStatus;
    this.admin = admin;
    this.dataEngine = dataEngine;
    asyncService = Executors.newSingleThreadExecutor();
    failureReportLogger = new FailureReportLogger();
    lastFailureReportCheckTime = System.currentTimeMillis();
    cassandraErrorCount = 0;
    isShutdown = false;
  }

  /**
   * Package local constructor used for testing only as of now.
   *
   * <p>In the future, make getNodeName of tfosUtils mockable as the call takes 5-10 seconds to
   * return, which is unacceptable for unit tests.
   */
  SystemMonitor(
      ServiceStatus serviceStatus,
      AdminInternal admin,
      DataEngine dataEngine,
      int intervalInSecs,
      BiosModules parentModules) {
    super(intervalInSecs, intervalInSecs, TimeUnit.SECONDS, null, parentModules);

    this.serviceStatus = serviceStatus;
    this.admin = admin;
    this.dataEngine = dataEngine;
    asyncService = Executors.newSingleThreadExecutor();
    failureReportLogger = new FailureReportLogger();
    lastFailureReportCheckTime = System.currentTimeMillis();
    cassandraErrorCount = 0;
    isShutdown = false;
  }

  /** Change the system to shutdown mode. */
  @Override
  public void shutdown() {
    isShutdown = true;
    asyncService.shutdownNow();
  }

  /**
   * Main program of the System Monitor component.
   *
   * <p>The method is invoked periodically to monitor the system.
   *
   * <p>If the node is marked down via SharedConfig, the method checks if there is any corresponding
   * error report. If there is none, it means the node has been restarted after the last failure, so
   * will just remove the failure mark. If there is any corresponding report, the method reloads all
   * AdminInternal and Context cache. The mark would be removed after the reload is done.
   */
  @Override
  public void runMaintenance() {
    if (isShutdown) {
      return;
    }
    if (serviceStatus.isInMaintenance()) {
      return;
    }
    failureReportLogger.tryLogging();
    try {
      final var maintenanceState =
          new GenericExecutionState("System Monitoring", ExecutorManager.getSidelineExecutor());
      maintenanceState.setTenantName(BiosConstants.TENANT_SYSTEM);
      maintenanceState.addHistory("start");
      long now = System.currentTimeMillis();
      List<Event> errors = extractFailureReports(lastFailureReportCheckTime, now, maintenanceState);
      lastFailureReportCheckTime = now;
      logger.debug(
          "{} failure reports were found since the last failure checkpoint", errors.size());
      for (Event error : errors) {
        try {
          final String failedNode = String.valueOf(error.get(BiosConstants.ATTR_NODE_ID));
          if (serviceStatus.getMyNodeName().equals(failedNode)) {
            logger.error(
                "Found a failure report. Restarting the server; node={}, reporter={}, reason={}",
                serviceStatus.getMyNodeName(),
                error.get(BiosConstants.ATTR_REPORTER),
                error.get(BiosConstants.ATTR_REASON));
            final long pid = ProcessHandle.current().pid();
            Runtime.getRuntime().exec(String.format("kill -TERM %d", pid));
            break;
          } else if (failedNode.equals("null")) {
            logger.error("Invalid failure report was found; record={}", error);
          } else {
            logger.debug("Failure report for another node: {}", error);
          }
        } catch (Throwable t) {
          logger.error(
              "Unexpected error happened during handling a failure report; record={}", error, t);
        }
      }
      cassandraErrorCount = 0;
      maintenanceState.addHistory("end");
    } catch (Throwable t) {
      final Throwable cause = t.getCause();
      if ((CassandraConnection.isRecoverable(t) || CassandraConnection.isRecoverable(cause))
          && cassandraErrorCount++ < CassandraConstants.MAX_ERROR_SUCCESSION) {
        logger.warn(
            "Maintenance failed due to a recoverable Cassandra error;"
                + " Backing off until the next cycle",
            t);
      } else {
        logger.error("An error happened during maintenance work", t);
      }
    }
  }

  protected boolean recover() {
    logger.info("Reloading server states");
    admin.reload();
    return true;
  }

  // TODO(Naoki): consolidate with RollupWorker.extractEvents. The best strategy should be to move
  //  this method to DataEngine.
  protected List<Event> extractFailureReports(long start, long end, ExecutionState maintenanceState)
      throws Throwable {
    ExtractRequest request = new ExtractRequest();
    request.setStartTime(start);
    request.setEndTime(end);

    final StreamDesc streamDesc =
        admin.getStream(BiosConstants.TENANT_SYSTEM, BiosConstants.STREAM_FAILURE_REPORT);

    final var state =
        new ExtractState("FetchFailureReport", maintenanceState, () -> new EventFactory() {});
    state.setStreamName(streamDesc.getName());
    state.setStreamDesc(streamDesc);
    state.setInput(request);
    state.setIndexQueryEnabled(false);
    state.setRollupTask(false);

    final var future = new CompletableFuture<List<Event>>();

    dataEngine.extractEvents(
        state,
        (events) -> {
          state.addHistory("}");
          logger.trace("{} was done successfully.\n{}", state.getExecutionName(), state);
          state.markDone();
          if (events == null) {
            logger.warn("EVENTS ARE NULL");
            future.complete(List.of());
          } else {
            future.complete(events);
          }
        },
        future::completeExceptionally);

    try {
      return future.get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Override
  public String healthCheck() {
    if (serviceStatus.isInMaintenance()) {
      return "In Maintenance Mode";
    }
    return "";
  }
}
