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
package io.isima.bios.execution.cores.bios;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.LiteBlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import java.util.concurrent.Executor;

public class BiosExecutor implements Executor {

  public static enum WaitStrategyKind {
    SPIN,
    BLOCK,
    LITE_BLOCK // TODO: can be more
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private final WaitStrategy waitStrategy;
  private final WaitStrategyKind waitStrategyKind;
  private final Disruptor<TaskEvent> disruptor;
  private final RingBuffer<TaskEvent> ringBuffer;

  protected BiosExecutor(
      String groupName, String executorName, int queueLength, WaitStrategyKind waitStrategyKind) {
    this.waitStrategyKind = waitStrategyKind;
    switch (waitStrategyKind) {
      case SPIN:
        waitStrategy = new BusySpinWaitStrategy();
        break;
      case BLOCK:
        waitStrategy = new BlockingWaitStrategy();
        break;
      case LITE_BLOCK:
        waitStrategy = new LiteBlockingWaitStrategy();
        break;
      default:
        throw new UnsupportedOperationException("Unknown wait strategy type " + waitStrategyKind);
    }
    int ql = 2;
    while (ql < queueLength) {
      ql *= 2;
    }
    final var threadFactory =
        new ThreadFactoryBuilder()
            .setNameFormat(
                String.format(
                    "bios-%s-exec-%s-%s",
                    waitStrategyKind.name().toLowerCase(), groupName, executorName))
            .setDaemon(true)
            .build();
    disruptor =
        new Disruptor<>(TaskEvent::new, ql, threadFactory, ProducerType.MULTI, waitStrategy);
    disruptor.handleEventsWith((event, sequence, endOfBatch) -> event.task.run());
    ringBuffer = disruptor.start();
  }

  @Override
  public void execute(Runnable task) {
    ringBuffer.publishEvent((event, sequence, receivedTask) -> event.task = receivedTask, task);
  }

  public void shutdown() {
    disruptor.shutdown();
  }

  public int getQueueLength() {
    return ringBuffer.getBufferSize();
  }

  public WaitStrategy getWaitStrategy() {
    return waitStrategy;
  }

  @Override
  public String toString() {
    return new StringBuilder(this.getClass().getSimpleName())
        .append("@")
        .append(Integer.toHexString(this.hashCode()))
        .append("; waitStrategy=")
        .append(waitStrategyKind)
        .append(", bufferSize=")
        .append(getQueueLength())
        .toString();
  }

  public static class Builder {
    private String groupName = "unknown";
    private String executorName = "unknown";
    private int queueLength = 16384;
    private WaitStrategyKind waitStrategyKind = WaitStrategyKind.BLOCK;

    public Builder groupName(String groupName) {
      this.groupName = groupName;
      return this;
    }

    public Builder executorName(String executorName) {
      this.executorName = executorName;
      return this;
    }

    public Builder queueLength(int queueLength) {
      this.queueLength = queueLength;
      return this;
    }

    public Builder waitStrategy(WaitStrategyKind strategyKind) {
      this.waitStrategyKind = strategyKind;
      return this;
    }

    public BiosExecutor build() {
      return new BiosExecutor(groupName, executorName, queueLength, waitStrategyKind);
    }
  }
}
