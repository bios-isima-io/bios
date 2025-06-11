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
package io.isima.bios.execution;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.isima.bios.configuration.Bios2Config;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.framework.BiosModule;
import io.isima.bios.utils.Utils;
import java.util.ArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class that manages executors and threads used in the BIOS server. */
public class ExecutorManager implements BiosModule {
  private static final Logger logger = LoggerFactory.getLogger(ExecutorManager.class);

  protected static final Executor sidelineExecutor =
      Executors.newCachedThreadPool(makeThreadFactory("sideline", Thread.NORM_PRIORITY));

  public static final String IO_THREAD_NAME = "bios-io";
  public static final String DIGESTOR_THREAD_NAME = "digestor";
  public static final String ADMIN_THREAD_NAME = "admin";

  // Thread management
  private int ioNumThreads;
  private int[] ioAffinityCpus;
  private int digestorNumThreads;
  private int[] digestorAffinityCpus;

  public ExecutorManager() {

    // Setup threads
    try {
      ioNumThreads = Bios2Config.ioNumThreads();
      ioAffinityCpus = parseAffinityCpus(Bios2Config.ioThreadsAffinity());
      ioNumThreads =
          checkThreadConfig(
              Bios2Config.IO_NUM_THREADS, Bios2Config.IO_THREADS_AFFINITY, "I/O", ioAffinityCpus);

      digestorNumThreads = Bios2Config.digestorNumThreads();
      digestorAffinityCpus = parseAffinityCpus(Bios2Config.digestorThreadsAffinity());
      digestorNumThreads =
          checkThreadConfig(
              Bios2Config.DIGESTOR_NUM_THREADS,
              Bios2Config.DIGESTOR_THREADS_AFFINITY,
              "Digestor",
              digestorAffinityCpus);
    } catch (ApplicationException e) {
      throw new RuntimeException(e);
    }
  }

  private int checkThreadConfig(
      String confKeyNumThreads, String confKeyAffinity, String threadsType, int[] affinityCpus)
      throws ApplicationException {
    int numThreads = Bios2Config.getInteger(confKeyNumThreads, 0);
    if (numThreads <= 0) {
      if (affinityCpus.length > 0) {
        logger.info("CPUs {} are assigned to {} threads", affinityCpus, threadsType);
        return affinityCpus.length;
      } else {
        throw new ApplicationException(
            String.format(
                "Either one of configuration parameters %s or %s must be set",
                confKeyNumThreads, confKeyAffinity));
      }
    } else if (affinityCpus.length > 0) {
      throw new ApplicationException(
          String.format(
              "Both of configuration parameters %s and %s must not be set -- set only either one",
              confKeyNumThreads, confKeyAffinity));
    } else {
      logger.info("{} non-affinity {} threads are assigned", numThreads, threadsType);
    }
    return numThreads;
  }

  private int[] parseAffinityCpus(String src) throws ApplicationException {
    if (src.isBlank()) {
      return new int[0];
    }
    final var cpus = new ArrayList<Integer>();
    final var tokens = src.split(",");
    for (var token : tokens) {
      final var elements = token.split("-", 2);
      try {
        final var first = Integer.parseInt(elements[0].trim());
        cpus.add(first);
        if (elements.length > 1) {
          final var second = Integer.parseInt(elements[1].trim());
          if (second < first) {
            throw new ApplicationException(
                "Syntax error in affinity CPUs specification: "
                    + token
                    + ": right must not be less than left");
          }
          for (int i = first + 1; i <= second; ++i) {
            cpus.add(i);
          }
        }
      } catch (NumberFormatException e) {
        throw new ApplicationException("Syntax error in affinity CPUs specification: " + src, e);
      }
    }
    final var result = new int[cpus.size()];
    for (int i = 0; i < result.length; ++i) {
      result[i] = cpus.get(i).intValue();
    }
    return result;
  }

  /**
   * Returns number of io threads.
   *
   * @return Number of io threads.
   */
  public int getIoNumThreads() {
    return ioNumThreads;
  }

  /**
   * Creates a thread factory for io.
   *
   * <p>This method creates a new instance of thread factory. The created instances are not
   * identical when you call the method multiple times.
   *
   * @return New thread factory for io
   */
  public ThreadFactory createIoThreadFactory() {
    return makeThreadFactory(IO_THREAD_NAME, Thread.MAX_PRIORITY, ioAffinityCpus);
  }

  /**
   * Returns number of digestor threads.
   *
   * @return Number of digestor threads.
   */
  public int getDigestorNumThreads() {
    return digestorNumThreads;
  }

  /**
   * Creates a thread factory for digestor.
   *
   * <p>This method creates a new instance of thread factory. The created instances are not
   * identical when you call the method multiple times.
   *
   * @return New thread factory for digestor
   */
  public ThreadFactory createDigestorThreadFactory() {
    return makeThreadFactory(DIGESTOR_THREAD_NAME, Thread.MIN_PRIORITY, digestorAffinityCpus);
  }

  /**
   * Creates a thread factory for tentative AdminInternal component.
   *
   * <p>This provides a tentative class of executors to execute old synchrnous AdminInternal
   * methods.
   *
   * @return New thread factory for admin
   */
  public ThreadFactory createAdminThreadFactory() {
    return makeThreadFactory(ADMIN_THREAD_NAME, Thread.NORM_PRIORITY, null);
  }

  private ThreadFactory makeThreadFactory(String name, int priority, int[] affinityCpus) {
    if (affinityCpus != null && affinityCpus.length > 0) {
      return new BiosAffinityThreadFactory(name, priority, affinityCpus);
    }
    return makeThreadFactory(name, priority);
  }

  public static ThreadFactory makeThreadFactory(String name, int priority) {
    return new ThreadFactoryBuilder().setNameFormat(name + "-%d").setPriority(priority).build();
  }

  /** Checks whether it is I/O thread that the method is currently in. */
  public static boolean isInIoThread() {
    return Utils.getCurrentThreadName().startsWith(IO_THREAD_NAME);
  }

  public static Executor getSidelineExecutor() {
    return sidelineExecutor;
  }

  @Override
  public void shutdown() {}
}
