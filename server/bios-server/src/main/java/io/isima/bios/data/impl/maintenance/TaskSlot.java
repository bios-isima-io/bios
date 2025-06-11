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
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;

/**
 * Class to help acquiring and releasing a task slot maintained by an AsyncSemaphore in a
 * synchronous code path.
 *
 * <p><i>Task Slots</i> are for controlling concurrency of maintenance tasks. The tasks can be
 * either synchronous or asynchronous. A maintenance task is required to acquire a slot from task
 * slots before starting, and is required to release the slot on leaving.
 *
 * <p>This class ensures acquiring and releasing a task slot when running a synchronous task by
 * writing the code such as:
 *
 * <pre>
 *   try (final var ignored = new TaskSlot(taskSlots, logger)) {
 *     // write task procedure here
 *   } catch (InterruptedException e) {
 *     // handle the exception here
 *   }
 * </pre>
 */
public class TaskSlot implements AutoCloseable {

  AsyncSemaphore taskSlots;

  public TaskSlot(AsyncSemaphore taskSlots, Logger logger) throws InterruptedException {
    try {
      taskSlots.acquire().toCompletableFuture().get();
      this.taskSlots = taskSlots;
    } catch (ExecutionException e) {
      logger.error("Error while acquiring a task slot", e);
    }
  }

  @Override
  public void close() {
    if (taskSlots != null) {
      taskSlots.release();
    }
  }
}
