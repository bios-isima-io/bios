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

import io.isima.bios.framework.BiosModule;
import java.util.concurrent.CompletionStage;

public interface InterNodeLock extends BiosModule {

  /**
   * Try acquiring an inter-node lock asynchronously.
   *
   * <p>The implementation must use the executor provided via the state object to execute async
   * stages.
   *
   * @param target Lock target name
   * @param state Execution state
   * @return Future for the lock. If the method was not able to aquire the lock, the future returns
   *     null as the lock value.
   */
  CompletionStage<Lock> acquireLock(String target, ExecutionState state);

  /**
   * Interface of classes that manages acquired locks.
   *
   * <p>The user must release the lock at the end of the operation with the state object provided
   * for {@link InterNodeLock#acquireLock()}.
   *
   * <p>The implementation must keep the state object and use it for refreshing and releasing the
   * lock.
   */
  interface Lock {

    /**
     * Queries whether the object's lock is valid or not.
     *
     * <p>The object becomes invalid when it is released or failed to refresh.
     *
     * @return
     */
    boolean isValid();

    CompletionStage<Void> refresh();

    CompletionStage<Void> unlock();
  }
}
