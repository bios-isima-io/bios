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

import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Helper class that provides functional interfaces that catch BIOS exceptions and rethrows them as
 * CompletionException's.
 *
 * <p>Useful for writing asynchronous business logic that throws BIOS exceptions.
 *
 * <p>NOTE: Using a helper in this class would add another layer of lambda (dynamic) function, which
 * generates some performance cost. Be careful not to call these methods too frequently in a
 * performance critical data path.
 */
public class ExecutionHelper {

  /**
   * Runner class that may throw TfosException or ApplicationException.
   *
   * <p>This class is useful to call a bios component method, most of which throw TfosException and
   * ApplicationException, by lambda expression.
   */
  @FunctionalInterface
  public interface BiosRunner {
    void run() throws TfosException, ApplicationException;
  }

  /**
   * Supplier class that may throw TfosException or ApplicationException.
   *
   * <p>This class is useful to call a bios component method, most of which throw TfosException and
   * ApplicationException, by lambda expression.
   *
   * @param <T> Class to supply
   */
  @FunctionalInterface
  public interface BiosSupplier<T> {
    T get() throws TfosException, ApplicationException;
  }

  public interface BiosAsyncTask<T> {
    CompletableFuture<T> executeAsync() throws TfosException, ApplicationException;
  }

  /**
   * A wrapper method that executes a BiosRunner instance.
   *
   * <p>The method catches an exception if thrown to convert it to CompletionException.
   *
   * @param runner The method runner
   */
  public static void run(BiosRunner runner) {
    try {
      runner.run();
    } catch (TfosException | ApplicationException e) {
      throw new CompletionException(e);
    }
  }

  /**
   * A wrapper method that executes a BiosSupplier instance.
   *
   * <p>The method catches an exception if thrown to convert it to CompletionException.
   *
   * @param <T> Class to supply
   * @param supplier The supplier to execute
   * @return The supplier result
   */
  public static <T> T supply(BiosSupplier<T> supplier) {
    try {
      return supplier.get();
    } catch (TfosException | ApplicationException e) {
      throw new CompletionException(e);
    }
  }

  public static <T> T sync(CompletionStage<T> future) throws ApplicationException, TfosException {
    try {
      return future.toCompletableFuture().get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ApplicationException("Interrupted", e);
    } catch (ExecutionException e) {
      final var cause = e.getCause();
      if (cause instanceof TfosException) {
        throw (TfosException) cause;
      }
      if (cause instanceof ApplicationException) {
        throw (ApplicationException) cause;
      }
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new RuntimeException(e);
    }
  }

  public static <S extends ExecutionState> Function<S, S> processStage(
      String stageName, BiosRunner runner) {
    return (state) -> {
      state.addHistory("(" + stageName);
      try {
        runner.run();
      } catch (TfosException | ApplicationException e) {
        state.markError();
        throw new CompletionException(e);
      }
      state.addHistory(")");
      return state;
    };
  }

  public static <S extends ExecutionState> Consumer<S> terminateStage(
      String stageName, BiosRunner runner) {
    return (state) -> {
      state.addHistory("(" + stageName);
      try {
        runner.run();
      } catch (TfosException | ApplicationException e) {
        throw new CompletionException(e);
      }
      state.addHistory(")");
    };
  }

  public static <S extends ExecutionState, T> Function<S, T> supplyWithState(
      String stageName, BiosSupplier<T> supplier) {
    return (state) -> {
      try {
        state.addHistory("(" + stageName);
        final var supply = supplier.get();
        state.addHistory(")");
        return supply;
      } catch (TfosException | ApplicationException e) {
        throw new CompletionException(e);
      }
    };
  }
}
