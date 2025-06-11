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
package io.isima.bios.data.synthesis;

import io.isima.bios.data.synthesis.generators.AttributeGeneratorsCreator;
import io.isima.bios.data.synthesis.generators.GeneratorResolver;
import io.isima.bios.data.synthesis.generators.RecordGenerator;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.maintenance.WorkerLock.LockEntity;
import io.isima.bios.models.Event;
import io.isima.bios.models.SignalConfig;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Per-stream synthetic data generator. */
public abstract class SignalSynthesizerBase {
  private final LockEntity lock;
  private final ScheduledFuture<?> publisherHandle;
  private final AtomicBoolean cancelled;

  protected final RecordGenerator generator;

  public SignalSynthesizerBase(
      String tenantName,
      SignalConfig signalConfig,
      GeneratorResolver resolver,
      LockEntity lock,
      ScheduledExecutorService scheduler,
      double publishesPerSecond)
      throws TfosException, ApplicationException {
    Objects.requireNonNull(tenantName);
    Objects.requireNonNull(signalConfig);
    Objects.requireNonNull(scheduler);
    if (publishesPerSecond <= 0 || publishesPerSecond > 100000) {
      throw new IllegalArgumentException(
          "Publishes per second must be more than 0 and less than 1000");
    }

    final var creator = AttributeGeneratorsCreator.getCreator(tenantName, signalConfig);
    if (resolver != null) {
      creator.externalResolver(resolver);
    }
    generator = new RecordGenerator(creator);
    this.lock = lock;
    long intervalMicros = (long) (1.0 / publishesPerSecond * 1.e6);
    publisherHandle =
        scheduler.scheduleAtFixedRate(
            () -> publish(), intervalMicros, intervalMicros, TimeUnit.MICROSECONDS);
    cancelled = new AtomicBoolean(false);
  }

  public void stop() {
    if (publisherHandle == null) {
      return;
    }
    if (!cancelled.getAndSet(true)) {
      publisherHandle.cancel(false);
      if (lock != null) {
        lock.close();
      }
    }
  }

  public Event generate() {
    return generator.generate();
  }

  protected abstract void publish();
}
