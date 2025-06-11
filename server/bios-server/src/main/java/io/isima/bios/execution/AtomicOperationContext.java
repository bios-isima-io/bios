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

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Statement;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;

/**
 * Class to track atomic data write.
 *
 * <p>A context object should be created at the beginning of an atomic data write operation(s). Such
 * an operation should accumulate write statements in the context object rather than executing them
 * in place. At the final phase of the operation(s), the statements are executed using atomic batch
 * mutation.
 *
 * <p>A context may contain multiple operations, for example, inserting to multiple signals. In
 * order to track such multi-operation use cases, this class tracks multiple async operations by
 * using an atomic integer.
 */
@Getter
public class AtomicOperationContext {

  private final UUID atomicOperationId;

  private final BatchStatement batchStatement = new BatchStatement();

  /** Updating cache, etc. */
  private final Set<Runnable> postOperations = ConcurrentHashMap.newKeySet();

  /** Used by bundled operations to wait for atomic operation completion. */
  private final Set<CompletableFuture<Void>> pendingCompletions = ConcurrentHashMap.newKeySet();

  /** Number of pending operations to complete. */
  private final AtomicInteger remainingOperations;

  /** Flag to indicate that any of bundled operations failed. */
  private final AtomicBoolean failed = new AtomicBoolean(false);

  /** Number of pending operations to pick up this object. */
  private final AtomicInteger operationsToStart;

  /**
   * @param atomicOperationId Atomic operation bundle ID
   * @param numBundledRequests Number of requrests bundled by the created context
   */
  public AtomicOperationContext(UUID atomicOperationId, int numBundledRequests) {
    this.atomicOperationId = atomicOperationId;
    remainingOperations = new AtomicInteger(numBundledRequests);
    operationsToStart = new AtomicInteger(numBundledRequests);
  }

  /** Adds a statement to be executed on commit. */
  public AtomicOperationContext addStatement(Statement statement) {
    batchStatement.add(statement);
    return this;
  }

  /**
   * Adds a task that should be executed after the successful batch mutation.
   *
   * <p>Constraints are:
   *
   * <ul>
   *   <li>The task must be synchronous (which implies no I/O involved)
   *   <li>The task must not depend on other tasks (post-op execution order is not controlled)
   *   <li>The task must not throw an exception (again, which implies no I/O involved)
   * </ul>
   */
  public AtomicOperationContext addPostOperation(Runnable task) {
    postOperations.add(task);
    return this;
  }

  /**
   * Starts a bundled operation and see whether it is the last one to start.
   *
   * @return Whether the operation is the last one to start
   */
  public boolean startEachOperation() {
    return operationsToStart.decrementAndGet() == 0;
  }

  /**
   * Completes a bundled operation to see if the accumulated tasks are ready to commit.
   *
   * <p>The class user should ensure this method is called once after successful operation. If the
   * operation fails, do not call this method so that final commit is never executed.
   *
   * @return Whether the accumulated statements are ready to execute
   */
  public boolean completeOperation(CompletableFuture<Void> pending) {
    pendingCompletions.add(pending);
    return remainingOperations.decrementAndGet() == 0;
  }

  /**
   * Aborts the atomic operation.
   *
   * @return Whether the operation is the last one to finish
   */
  public boolean failOperation() {
    failed.set(true);
    return remainingOperations.decrementAndGet() == 0;
  }
}
