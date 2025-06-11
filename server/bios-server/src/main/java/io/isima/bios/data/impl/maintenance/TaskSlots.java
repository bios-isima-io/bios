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
package io.isima.bios.data.impl.maintenance;

import com.ibm.asyncutil.locks.AsyncSemaphore;
import com.ibm.asyncutil.locks.FairAsyncSemaphore;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskSlots {
  private static final Logger logger = LoggerFactory.getLogger(TaskSlots.class);

  @Getter final AsyncSemaphore slots;

  public TaskSlots(int numTaskSlots) {
    slots = new FairAsyncSemaphore(numTaskSlots);
  }

  public TaskSlot acquireSlot(Logger logger) throws InterruptedException {
    return new TaskSlot(slots, logger);
  }

  public CompletionStage<Void> runAsyncTask(
      Supplier<CompletionStage<Void>> task, Executor executor, String taskName) {
    logger.debug(
        "starting a task; taskName={}, availableSlots={}", taskName, slots.getAvailablePermits());
    return slots
        .acquire()
        .thenComposeAsync((none) -> task.get(), executor)
        .whenComplete(
            (none, t) -> {
              slots.release();
              logger.debug(
                  "finished a task; taskName={}, availableSlots={}",
                  taskName,
                  slots.getAvailablePermits());
              if (t instanceof CompletionException) {
                throw (CompletionException) t;
              } else if (t != null) {
                throw new CompletionException(t);
              }
            });
  }
}
