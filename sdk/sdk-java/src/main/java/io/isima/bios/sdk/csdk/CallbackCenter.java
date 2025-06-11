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
package io.isima.bios.sdk.csdk;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Purpose of this class is to run callback methods invoked by C-SDK. All the resources of this
 * class (i.e., class and methods) are static so that the JNI layer can cache them safely. It saves
 * amount of time to resolve callback resources.
 */
public class CallbackCenter {
  // This executor / thread pool is used for executing callback methods asynchronously.
  // The callback method may be called from the C-SDK in some I/O thread. We want to return such
  // methods quickly. But some of the completion tasks may be slow. We, thus, run a callback
  // execution in a different thread.
  private static final Executor executor = makeExecutor();

  private static Executor makeExecutor() {
    int completionThreadPoolSize = 2;

    final var src = System.getProperty("io.isima.sdk.numCompletionThreads");
    if (src != null) {
      try {
        int value = Integer.valueOf(src);
        if (value >= 1 && value <= 128) {
          // use the value only when it's in this allowed range
          completionThreadPoolSize = value;
        }
      } catch (NumberFormatException e) {
        // we can't log, ignore silently
      }
    }

    return Executors.newFixedThreadPool(
        completionThreadPoolSize,
        new ThreadFactory() {
          final ThreadGroup threadGroup = new ThreadGroup("bios-client");
          final AtomicInteger index = new AtomicInteger();

          @Override
          public Thread newThread(Runnable r) {
            final var currentIndex = index.getAndIncrement();
            final var thread = new Thread(threadGroup, r, threadGroup.getName() + currentIndex);
            thread.setDaemon(true);
            return thread;
          }
        });
  }

  /**
   * Runs {@link CSdkApiCaller#onCompletion}.
   *
   * @param callbackObject The instance of AsyncNativeCall.
   * @param endpoint Endpoint string or null if unavailable
   * @param statusCode Operation status code.
   * @param payload Reply payload.
   * @param length Length of the payload.
   * @param timestampPosition Current position of the timestamp buffer
   * @param csdkLatency csdk latency in microseconds
   */
  public static void onCompletion(
      CSdkApiCaller<?> callbackObject,
      int statusCode,
      String endpoint,
      ByteBuffer payload,
      long length,
      long timestampPosition,
      long csdkLatency,
      long qosRetryConsidered,
      long qosRetrySent,
      long qosRetryResponseUsed) {
    executor.execute(
        () -> {
          if (payload != null) {
            payload.clear();
            payload.limit((int) length);
          }
          callbackObject.onCompletion(
              statusCode,
              endpoint,
              payload,
              timestampPosition,
              csdkLatency,
              qosRetryConsidered,
              qosRetrySent,
              qosRetryResponseUsed);
        });
  }

  /**
   * Runs {@link InsertBulkExecutor#more}.
   *
   * @param call The callback object.
   * @param contextId IngestBulk operation context ID.
   * @param fromIndex The index where the request batch starts, inclusive.
   * @param toIndex The index where the request batch ends, exclusive.
   */
  public static void moreIngestBulk(
      InsertBulkExecutor call, long contextId, int fromIndex, int toIndex) {
    executor.execute(() -> call.more(contextId, fromIndex, toIndex));
  }
}
