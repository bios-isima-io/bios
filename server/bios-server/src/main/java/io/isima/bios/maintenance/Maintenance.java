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

import io.isima.bios.execution.ExecutorManager;
import io.isima.bios.utils.StringUtils;
import io.isima.bios.utils.Utils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Maintenance {
  private static final Logger logger = LoggerFactory.getLogger(Maintenance.class);

  private static final int POOL_SIZE = 4;
  private static final int SHUTDOWN_TIMEOUT_INITIAL = 10000;
  private static final int SHUTDOWN_TIMEOUT_TOTAL = 40000;

  // We run only one maintenance task at a time to prevent Cassandra getting overloaded
  @Getter
  private final ScheduledThreadPoolExecutor scheduler =
      (ScheduledThreadPoolExecutor)
          Executors.newScheduledThreadPool(
              POOL_SIZE, ExecutorManager.makeThreadFactory("bios-maint", Thread.MIN_PRIORITY));

  /** Registered maintenance workers. */
  private final List<MaintenanceWorker> workers = new ArrayList<>();

  /** Scheduled maintenance tasks. Indexes are corresponding to the ones of the workers list. */
  private final List<ScheduledFuture<?>> tasks = new ArrayList<>();

  private volatile boolean shutdown = false;

  /** The ctor. */
  public Maintenance() {
    scheduler.setRemoveOnCancelPolicy(true);
    scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
  }

  /**
   * Register a maintenance worker.
   *
   * <p>This method does not start maintenance task. Use method {@link #start()} to invoke
   * maintenance tasks.
   *
   * @param worker The maintenance worker to register.
   */
  public void register(MaintenanceWorker worker) {
    logger.debug("registering maintenance worker {}", worker);
    workers.add(worker);
  }

  /**
   * Unregister a maintenance worker.
   *
   * <p>The method also cancels scheduled task of the worker.
   *
   * @param worker The maintenance worker to unregister.
   * @return true if the un-registration is successful, false otherwise.
   */
  public boolean unregister(MaintenanceWorker worker) {
    for (int i = 0; i < workers.size(); ++i) {
      final MaintenanceWorker current = workers.get(i);
      if (current == worker) {
        logger.info("unregistering maintenance worker {}", worker);
        tasks.get(i).cancel(false);
        workers.remove(i);
        tasks.remove(i);
        return true;
      }
    }
    logger.warn("failed to unregister maintenance worker {}", worker);
    return false;
  }

  /** Starts all registered maintenance workers. */
  public void start() {
    if (shutdown) {
      logger.error("Maintenance is shutdown. Cannot use it again");
      return;
    }
    logWorkers();
    for (MaintenanceWorker worker : workers) {
      tasks.add(
          scheduler.scheduleWithFixedDelay(
              worker, worker.getInitialDelay(), worker.getInterval(), worker.getTimeUnit()));
    }
  }

  /** Stops all scheduled services. */
  public void stop() {
    if (shutdown) {
      logger.error("Maintenance is shutdown. Cannot use it again");
      return;
    }
    tasks.forEach(task -> task.cancel(false));
    tasks.clear();
  }

  /**
   * Terminates all maintenance workers.
   *
   * <p>The method also stops all scheduled tasks.
   */
  public void shutdown() {
    if (!shutdown) {
      workers.forEach(worker -> worker.shutdown());
      stop();
      scheduler.shutdown();
      boolean terminated = false;
      final var shutdownStartTime = System.currentTimeMillis();
      final var shutdownWaitUntil = shutdownStartTime + SHUTDOWN_TIMEOUT_TOTAL;
      try {
        logger.info("Maintenance shutting down; activeCount={}", scheduler.getActiveCount());
        terminated = scheduler.awaitTermination(SHUTDOWN_TIMEOUT_INITIAL, TimeUnit.MILLISECONDS);
        if (!terminated) {
          logger.info("Maintenance shutting down 2; activeCount={}", scheduler.getActiveCount());
          scheduler.shutdownNow();
          logger.info("Maintenance shutting down 3; activeCount={}", scheduler.getActiveCount());
          terminated =
              scheduler.awaitTermination(
                  shutdownWaitUntil - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
          logger.info("Maintenance shutting down 4; activeCount={}", scheduler.getActiveCount());
        }
      } catch (InterruptedException e) {
        logger.warn("Maintenance shutdown interrupted", e);
        Thread.currentThread().interrupt();
      } catch (Throwable t) {
        logger.warn("Got exception when shutting down maintenance.", t);
        // we are shutting down..ignore any errors
      }
      if (!terminated) {
        logger.warn(
            "shutdown maintenance timed out; activeCount={}, shutdownStartTime={}, "
                + "shutdownWaitUntil={}",
            scheduler.getActiveCount(),
            StringUtils.tsToIso8601(shutdownStartTime),
            StringUtils.tsToIso8601(shutdownWaitUntil));
        logger.info("Thread dump: {}", Utils.dumpAllThreads());
      }
    }
    shutdown = true;
  }

  public void runSingleTaskWithDelay(Runnable task, long delayTime, TimeUnit unit) {
    scheduler.schedule(task, delayTime, unit);
  }

  /** Log registered workers. */
  private void logWorkers() {
    logger.info("==== Registered Maintenance Workers ===============");
    for (MaintenanceWorker worker : workers) {
      logger.info(
          "{}: interval={} {}, initial={} {}",
          worker.getClass().getSimpleName(),
          worker.getInterval(),
          worker.getTimeUnit(),
          worker.getInitialDelay(),
          worker.getTimeUnit());
    }
    logger.info("===================================================");
  }
}
