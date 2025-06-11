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
package io.isima.bios.sdk.impl;

import io.isima.bios.exceptions.InsertBulkFailedException;
import io.isima.bios.sdk.errors.BiosClientError;
import io.isima.bios.sdk.exceptions.BiosClientException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class BiosClientUtils {

  /**
   * Resolve client error code by status code (error number).
   *
   * @param statusCode Status code / error number.
   * @return Corresponding error code
   * @throws BiosClientException when there is no corresponding error code
   */
  public static BiosClientError statusToError(int statusCode) throws BiosClientException {
    for (BiosClientError status : BiosClientError.values()) {
      if (status.getErrorNumber() == statusCode) {
        return status;
      }
    }
    throw new BiosClientException("Unknown status code: 0x" + Integer.toHexString(statusCode));
  }

  /**
   * Waits on a completable future synchronously and returns the result.
   *
   * <p>All caught exceptions would be converted to {@link BiosClientException}.
   *
   * @param future The future to wait on.
   * @return The result given by the future.
   * @throws BiosClientException thrown to indicate that the future was completed exceptionally.
   */
  public static <T> T wait(CompletableFuture<T> future) throws BiosClientException {
    try {
      return future.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      final var cause = e.getCause();
      if (cause instanceof InsertBulkFailedException) {
        final var orig = (InsertBulkFailedException) cause;
        throw new InsertBulkFailedException(orig);
      } else if (cause instanceof BiosClientException) {
        final var orig = (BiosClientException) cause;
        throw new BiosClientException(orig.getCode(), orig.getMessage(), orig.getEndpoint(), orig);
      } else if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      } else {
        throw new BiosClientException(cause);
      }
    }
    return null;
  }
}
