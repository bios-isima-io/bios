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
package io.isima.bios;

import static java.lang.Thread.sleep;

public class TestUtils {
  /** System.sleep() can get interrupted; this method compensates for that. */
  @SuppressWarnings({"BusyWait", "checkstyle:EmptyCatchBlock"})
  public static void sleepAtLeast(long milliseconds) {
    final long beforeSleep = System.currentTimeMillis();
    while (System.currentTimeMillis() <= beforeSleep + milliseconds) {
      try {
        sleep(milliseconds); // While loop to compensate for interruptions.
      } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
