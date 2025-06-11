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
package io.isima.bios.execution.cores;

import static org.junit.Assert.assertEquals;

import com.lmax.disruptor.util.DaemonThreadFactory;
import io.isima.bios.execution.cores.bios.BiosExecutor;
import io.isima.bios.execution.cores.bios.BiosExecutor.WaitStrategyKind;
import io.netty.channel.nio.NioEventLoopGroup;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

/** Unit test for simple App. */
public class ExecutorTest {

  private static final int size = 100000;

  @Test
  public void testFixedThreadPool() throws Exception {
    final var executor = Executors.newFixedThreadPool(1);
    testIteration("fixed thread", executor);
  }

  @Test
  public void testNettyExecutor() throws Exception {
    final var executor = new NioEventLoopGroup(1, DaemonThreadFactory.INSTANCE);
    testIteration("Netty executor", executor);
  }

  @Test
  public void testBlockWait() throws Exception {
    final var executor =
        BiosExecutor.newBuilder()
            .queueLength(size * 3)
            .waitStrategy(WaitStrategyKind.BLOCK)
            .build();
    testIteration("block wait", executor);
  }

  @Test
  public void testSpinWait() throws Exception {
    final var executor =
        BiosExecutor.newBuilder().queueLength(size * 3).waitStrategy(WaitStrategyKind.SPIN).build();
    testIteration("spin wait", executor);
  }

  @Test
  public void testLiteBlockingWait() throws Exception {
    final var executor =
        BiosExecutor.newBuilder()
            .queueLength(size * 3)
            .waitStrategy(WaitStrategyKind.LITE_BLOCK)
            .build();
    testIteration("lite block", executor);
  }

  private void testIteration(String name, Executor executor)
      throws InterruptedException, ExecutionException {
    final int nthreads = 20;
    final var results = new Long[size];
    final CompletableFuture<Void> fut = new CompletableFuture<>();
    executor.execute(() -> System.out.print(String.format("%20s : ", name)));
    final AtomicInteger count = new AtomicInteger();
    final long start = System.nanoTime();
    for (long i = 0; i < nthreads; ++i) {
      final long ivalue = i;
      new Thread() {

        @Override
        public void run() {
          for (long j = 0; j < size / nthreads; ++j) {
            final Long value = ivalue * size / nthreads + j;
            CompletableFuture.supplyAsync(
                    () -> {
                      return value + 1000;
                    },
                    executor)
                .thenApplyAsync(
                    (x) -> {
                      return x + 1000;
                    },
                    executor)
                .thenApplyAsync(
                    (x) -> {
                      return x + 1000;
                    },
                    executor)
                .thenAcceptAsync(
                    (x) -> {
                      results[value.intValue()] = x;
                      if (count.incrementAndGet() == size) {
                        fut.complete(null);
                      }
                    },
                    executor);
          }
        }
      }.run();
    }

    fut.get();
    final long end = System.nanoTime();

    System.out.println(String.format("%f ms", (double) (end - start) / 1000000));

    for (int i = 0; i < size; ++i) {
      assertEquals(Long.valueOf(i + 3000), results[i]);
    }
  }
}
