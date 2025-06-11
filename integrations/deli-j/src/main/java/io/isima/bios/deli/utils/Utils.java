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
package io.isima.bios.deli.utils;

import io.isima.bios.sdk.errors.BiosClientError;
import io.isima.bios.sdk.exceptions.BiosClientException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;

public class Utils {
  public static final Set<BiosClientError> RETRYABLE_ERROR =
      Set.of(BiosClientError.BAD_GATEWAY, BiosClientError.OPERATION_CANCELLED,
          BiosClientError.SERVER_IN_MAINTENANCE, BiosClientError.GENERIC_SERVER_ERROR,
          BiosClientError.TIMEOUT, BiosClientError.SERVICE_UNAVAILABLE,
          BiosClientError.SERVER_CHANNEL_ERROR, BiosClientError.SERVER_CONNECTION_FAILURE);


  /**
   * Executes a remote operation.
   *
   * <p>The method retries until timeout if the remote operation fails for retryable errors</p>
   */
  public static void executeRemoteOperation(String taskName, int initialSleepSeconds,
      int maxSleepSeconds, int timeoutSeconds, AtomicBoolean isShuttingDown, Logger logger,
      String logContext, RemoteAccessTask task)
      throws InterruptedException, BiosClientException {
    int sleepSeconds = initialSleepSeconds;
    long timeout = System.currentTimeMillis() + timeoutSeconds * 1000;
    while (true) {
      try {
        task.run();
        break;
      } catch (BiosClientException e) {
        if (Utils.RETRYABLE_ERROR.contains(e.getCode())) {
          if (timeoutSeconds == 0 || System.currentTimeMillis() < timeout) {
            logger.warn("{} failed, retrying in {} seconds; {}, error={}",
                taskName, sleepSeconds, logContext, e.getMessage());
            int remaining = sleepSeconds;
            while (--remaining >= 0) {
              if (isShuttingDown.get()) {
                logger.warn("Retry interrupted for shutdown; {}", logContext);
                throw new InterruptedException("shutdown");
              }
              Thread.sleep(1000);
            }
            if (sleepSeconds < maxSleepSeconds) {
              sleepSeconds *= 2;
            }
            continue;
          }
          logger.warn("{} failed, giving up after {} seconds; {}, error={}",
              taskName, timeoutSeconds, logContext, e.getMessage());
        }
        throw e;
      }
    }
  }

  /**
   * Reads the content of a file to a string.
   *
   * @param fileName File name
   * @return The content of the file if exists else null
   * @throws IOException thrown when reading the file fails
   */
  public static byte[] readFile(String fileName) throws IOException {
    final var file = new File(fileName);
    return file.exists() ? Files.readAllBytes(file.toPath()) : null;
  }

  /**
   * Copy a file if possible and necessary.
   *
   * <p>
   * The method copies a source file when all of following conditions meet:
   * </p>
   * <ul>
   *   <li>source file name is not null</li>
   *   <li>source file exists</li>
   *   <li>destination file name is not null</li>
   *   <li>destination file does not exist or destination file content is different from source</li>
   * </ul>
   *
   * @param srcFileName source file name, can be null
   * @param dstFileName destination file name, can be null
   * @return true if the destination file is overwritten
   * @throws IOException thrown when accessing file fails
   */
  public static boolean tryCopyFile(String srcFileName, String dstFileName)
      throws IOException {
    final var srcFile = srcFileName != null ? new File(srcFileName) : null;
    final var dstFile = dstFileName != null ? new File(dstFileName) : null;
    if (srcFile == null || dstFile == null || !srcFile.exists() || (dstFile.exists()
        && FileUtils.contentEquals(srcFile, dstFile))) {
      return false;
    }
    FileUtils.copyFile(srcFile, dstFile);
    return true;
  }

  /**
   * Save a content to a file if the content is given and is different from current file.
   */
  public static boolean trySaveToFile(byte[] content, String fileName) throws IOException {
    if (content == null) {
      return false;
    }
    final var dstContent = readFile(Objects.requireNonNull(fileName));
    if (Arrays.equals(content, dstContent)) {
      return false;
    }
    Files.write(new File(fileName).toPath(), content, StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING);
    return true;
  }

  /**
   * Save a content to a file if the content is given and is different from current file.
   */
  public static boolean trySaveToFile(String content, String fileName) throws IOException {
    if (content == null) {
      return false;
    }
    return trySaveToFile(content.getBytes(), fileName);
  }
}
