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
package io.isima.bios.utils;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
  private static final Logger logger = LoggerFactory.getLogger(Utils.class);

  private static final String nodeName;

  static {
    String node;
    try {
      node = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      node = "";
    }
    nodeName = node;
  }

  /**
   * Returns the application version string.
   *
   * @return Version string.
   */
  public static String version() {
    final String propertyFile = "/version.properties";
    String version = "";
    try (InputStream inputStream = Utils.class.getClassLoader().getResourceAsStream(propertyFile)) {
      if (inputStream == null) {
        return version;
      }
      Properties properties = new Properties();
      properties.load(inputStream);
      version = properties.getProperty("version");
    } catch (IOException e) {
      logger.error("Error in reading property file {}", propertyFile, e);
    }
    return version;
  }

  /**
   * Method to convert byte data to a hex string.
   *
   * <p>The output is accumulated into the specified string builder.
   *
   * @param data Input data
   * @param sb StringBuilder to accumulate the output.
   */
  public static void byteToHexString(final byte data, final StringBuilder sb) {
    sb.append(toHex((data & 0xff) >> 4)).append(toHex(data & 0xf));
  }

  private static char toHex(int src) {
    return (src < 10) ? (char) (src + '0') : (char) ((src - 10) + 'a');
  }

  public static String getNodeName() {
    return nodeName;
  }

  private static final long KCLOCK_OFFSET = 0x01b21dd213814000L;
  private static final long KCLOCK_MULTIPLIER_L = 10000L;

  /**
   * Converts a timestamp in UUID v1 to milliseconds since epoch.
   *
   * @param uuidTimestamp Timestamp in UUID v1
   * @return milliseconds since epoch
   */
  public static long uuidTimestampToMillis(long uuidTimestamp) {
    return (uuidTimestamp - KCLOCK_OFFSET) / KCLOCK_MULTIPLIER_L;
  }

  /**
   * Returns the timestamp in a v1 UUID.
   *
   * @param uuid UUID, this has to be a time-based UUID, which has version type 1.
   * @return Milliseconds since Epoch
   * @throws UnsupportedOperationException If this UUID is not a version 1 UUID
   */
  public static long uuidV1TimestampInMillis(UUID uuid) {
    return (uuid.timestamp() - KCLOCK_OFFSET) / KCLOCK_MULTIPLIER_L;
  }

  /**
   * Returns the timestamp in a v1 UUID.
   *
   * @param uuid UUID, this has to be a time-based UUID, which has version type 1.
   * @return Microseconds since Epoch
   * @throws UnsupportedOperationException If this UUID is not a version 1 UUID
   */
  public static long uuidV1TimestampInMicros(UUID uuid) {
    return (uuid.timestamp() - KCLOCK_OFFSET) / (KCLOCK_MULTIPLIER_L / 1000);
  }

  /** Returns the "floor" of an integer wrt a divisor, e.g. floor(127, 10) = 120. */
  public static long floor(long input, long divisor) {
    return input / divisor * divisor;
  }

  /**
   * Returns the "floor" of an integer wrt a divisor, offset to a particular value. e.g.
   * offsetFloor(127, 10, 105) = 125.
   */
  public static long offsetFloor(long input, long divisor, long offset) {
    return floor((input - offset), divisor) + offset;
  }

  /** Returns the "ceiling" of an integer wrt a divisor, e.g. floor(123, 10) = 130. */
  public static long ceiling(long input, long divisor) {
    return (input + divisor - 1) / divisor * divisor;
  }

  /** Rounds the input wrt a precision, e.g. round(127, 10) = 130. */
  public static long round(double input, long precision) {
    return Math.round(input / precision) * precision;
  }

  public static String dumpAllThreads() {
    StringBuilder threadDump = new StringBuilder(System.lineSeparator());
    ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
    for (ThreadInfo threadInfo : threadMxBean.dumpAllThreads(true, true)) {
      threadDump.append(threadInfo.toString());
    }
    return threadDump.toString();
  }

  public static String dumpThread(String threadName) {
    StringBuilder threadDump = new StringBuilder(System.lineSeparator());
    ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
    for (ThreadInfo threadInfo : threadMxBean.dumpAllThreads(true, true)) {
      if (threadInfo.getThreadName().equals(threadName)) {
        threadDump.append(threadInfo.toString());
      }
    }
    return threadDump.toString();
  }

  /** Gets current thread name. */
  public static String getCurrentThreadName() {
    return Thread.currentThread().getName();
  }
}
