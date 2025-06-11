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
package io.isima.bios.load.utils;

import java.net.InetAddress;
import java.util.Random;

import org.apache.commons.lang3.RandomUtils;
import org.apache.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import io.isima.bios.data.distribution.ExponentialGenerator;

public class ApplicationUtils {

  public static final String INGEST_THREAD_NAME = "Ingest Threads";
  public static final String EXTRACT_THREAD_NAME = "Extract Threads";
  public static final String CONTEXT_THREAD_NAME = "Context Threads";
  private static final Logger logger = Logger.getLogger(ApplicationUtils.class);

  private static final String HOST = "host";
  private static final String CLONE_SOURCE_HOST = "cloneSourceHost";
  private static final String CLONE_SOURCE_PORT = "cloneSourcePort";
  private static final String CLONE_SOURCE_EMAIL = "cloneSourceEmail";
  private static final String CLONE_SOURCE_PASSWORD = "cloneSourcePassword";
  private static final String PORT = "port";
  private static final String EMAIL = "email";
  private static final String PASSWORD = "password";
  private static final String SSL_FLAG = "sslEnabled";
  private static final String JMETER_CONFIG_FILE = "jmeterConfigFile";
  private static final String STREAM_PATH = "stream";
  private static final String JMETER_HOME = "jmeterHome";
  private static final String INSERT_THREADS = "insertThreads";
  private static final String SELECT_THREADS = "selectThreads";
  private static final String UPSERT_THREADS = "upsertThreads";
  private static final String MISC_THREAD = "miscThread";
  private static final String TEST_LOOP = "testLoop";
  private static final String INTERVAL = "jmeterConstantTimer";
  private static final String LOG_SKIP_MESSAGES = "logSkipMessages";
  private static final String MAX_CONTEXT_KEY = "maxContextSize";
  private static final String GAUSSIAN_UPPER_RANGE = "gaussianUpperRange";
  private static final String GAUSSIAN_LOWER_RANGE = "gaussianLowerRange";
  private static final String GAUSSIAN_TIME_WINDOW = "gaussianTimeWindow";
  private static final String LOAD_PATTERN = "loadPattern";
  private static final String ADMIN_PASSWORD = "adminPassword";
  private static final String LOG_METRICS_STREAM_NAME = "logMetricsStream";
  private static final String SELECT_INTERVAL = "selectDuration";
  private static final String SELECT_START_DELTA = "selectStartDelta";
  private static final String PUBLISH_JMETER_LOG = "publishJmeterLog";
  private static final String CLONE_EXISTING_TENANT_PATTERN = "cloneExistingPattern";
  private static final String TENANT_TO_CLONE = "tenantToClone";
  private static final String SYSTEM_ADMIN_USER = "systemAdminUser";
  private static final String SYSTEM_ADMIN_PASSWORD = "systemAdminPassword";
  private static final String EMPTY_STRING = "";
  private static final String LOAD_PERCENT = "loadPercent";


  public static String getHost() {
    return getString(HOST, "localhost");
  }

  public static String getCloneSourceHost() {
    return getString(CLONE_SOURCE_HOST, "localhost");
  }

  public static int getCloneSourcePort() {
    return getInt(CLONE_SOURCE_PORT, 443);
  }

  public static String getCloneSourceEmail() {
    return getString(CLONE_SOURCE_EMAIL, "extract");
  }

  public static String getCloneSourcePassword() {
    return getString(CLONE_SOURCE_PASSWORD, "extract");
  }

  public static int getPort() {
    return getInt(PORT, 443);
  }

  public static String getEmail() {
    return getString(EMAIL, "extract");
  }

  public static String getPassword() {
    return getString(PASSWORD, "extract");
  }

  public static boolean getSSLFlag() {
    return getBoolean(SSL_FLAG, true);
  }

  public static String getJmeterConfigFileName() {
    if (getString(JMETER_CONFIG_FILE, null) == null) {
      printUsageAndExit();
    }
    return getString(JMETER_CONFIG_FILE, null);
  }

  public static String getStreamJsonPath() {
    if (getString(STREAM_PATH, null) == null) {
      printUsageAndExit();
    }
    return getString(STREAM_PATH, null);
  }

  public static String getJmeterHome() {
    if (getString(JMETER_HOME, null) == null) {
      printUsageAndExit();
    }
    return getString(JMETER_HOME, ".");
  }

  public static String getJmeterPropFile() {
    return getJmeterHome() + "/bin/jmeter.properties";
  }

  public static int getIngestThreadNumber() {
    return getInt(INSERT_THREADS, 7);
  }

  public static int getExtractThreadNumber() {
    return getInt(SELECT_THREADS, 1);
  }

  public static int getContextThreadNumber() {
    return getInt(UPSERT_THREADS, 2);
  }

  public static int getMiscThreadNumber() {
    return getInt(MISC_THREAD, 1);
  }

  public static int getTestLoopCount() {
    return getInt(TEST_LOOP, -1);
  }

  public static String getTestPlanFileName() {
    return getJmeterConfigFileName() + ".jmx";
  }

  public static String getTestResultFileName() {
    return getJmeterConfigFileName() + ".jtl";
  }

  private static String getString(String key, String defaultValue) {
    String value = System.getProperty(key);
    return value != null ? value : defaultValue;
  }

  private static int getInt(String key, int defaultValue) {
    String value = System.getProperty(key);
    if (value == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new RuntimeException(e);
    }
  }

  private static Long getLong(String key, Long defaultValue) {
    String value = System.getProperty(key);
    if (value == null) {
      return defaultValue;
    }
    try {
      return Long.valueOf(value);
    } catch (NumberFormatException e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean getBoolean(String key, boolean defaultValue) {
    String value = System.getProperty(key);
    if (value == null) {
      return defaultValue;
    }
    return Boolean.parseBoolean(value);
  }

  private static void printUsageAndExit() {
    final String usageString =
        "Usage : java [options]"
            + " -jar tfos-load-generator.jar\n"
            + "options:\n"
            + "-Dhost=<hostname>         : Server host name or ip address"
            + " (optional, default=localhost)\n"
            + "-Dport=<portnumber>       : Port number of server (optional, default=443)\n"
            + "-DsslEnabled=<true/false> : True if server is ssl enabled, else false"
            + " (optional, default=true)\n"
            + "-Dtenant=<tenant>         : name of the tenant\n"
            + "-Dstream=<stream.json>    : load test specific stream config json path\n"
            + "-DjmeterHome=<jmeter_dir> : jmeter home path\n"
            + "-DingestThread=<n>        : Number of ingest task threads"
            + " (optional, default=7)\n"
            + "-DextractThread=<n>       : Number of extract task threads"
            + " (optional, default=1)\n"
            + "-DcontextThread=<n>       : Number of context task threads"
            + " (optional, default=2)\n"
            + "-Dinterval=<milliseconds> : Wait for specified "
            + "millisecond before sending next request (optional, default=0 (no wait))";

    System.err.println(usageString);
    System.exit(1);
  }

  @VisibleForTesting
  @Deprecated
  public static void initSystemProperty() {
    System.setProperty(SSL_FLAG, "false");
    System.setProperty(JMETER_HOME, "/Users/apple/Downloads/apache-jmeter-5.1.1");
    System.setProperty(JMETER_CONFIG_FILE, "load_test");
    System.setProperty(STREAM_PATH,
        "/Users/apple/load_test_distribution_test/python_script/load_streams.json");
  }

  public static String getJmeterConstantTimer() {
    return getString(INTERVAL, "0");
  }

  public static String[] getLogSkipMessages() {
    return getString(LOG_SKIP_MESSAGES, "").split(",");
  }

  public static Integer getMaxContextSize() {
    return getInt(MAX_CONTEXT_KEY, 1000000);
  }

  public static String getBrowserType() {
    return Enums.BrowserType.values()[RandomUtils.nextInt(0, Enums.BrowserType.values().length)]
        .name();
  }

  public static String getNetworkType() {
    return Enums.NetworkType.values()[RandomUtils.nextInt(0, Enums.NetworkType.values().length)]
        .name();
  }

  public static String getDeviceType() {
    return Enums.DeviceType.values()[RandomUtils.nextInt(0, Enums.DeviceType.values().length)]
        .name();
  }

  public static String getGender() {
    return Enums.Gender.values()[RandomUtils.nextInt(0, Enums.Gender.values().length)].name();
  }

  public static int getGausianLowerRange() {
    return getInt(GAUSSIAN_LOWER_RANGE, 0);
  }

  public static int getGausianUpperRange() {
    return getInt(GAUSSIAN_UPPER_RANGE, 0);
  }

  public static int getGaussianTimeWindow() {
    return (getInt(GAUSSIAN_TIME_WINDOW, 6) * 60) / 5;
  }

  public static LoadPattern getLoadPattern() {
    return LoadPattern.valueOf(getString(LOAD_PATTERN, "GAUSSIAN"));
  }

  public static String getAdminPassword() {
    return getString(ADMIN_PASSWORD, "admin");
  }

  public static String getComputerName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (Exception ex) {
      logger.error(ex.getMessage(), ex);
    }
    return "UNKNOWN";
  }


  public static String getLogMetricsStreamName() {
    return getString(LOG_METRICS_STREAM_NAME, null);
  }

  public static long getExtractInterval() {
    long extractDuration = getLong(SELECT_INTERVAL, Long.valueOf(20000));
    long extractStart = getLong(SELECT_START_DELTA, 0L);
    if (extractStart == 0L) {
      return (-1 * extractDuration);
    }
    return extractDuration;
  }

  public static long getExtractStartTimeDelta() {
    long extractStart = getLong(SELECT_START_DELTA, 0L);
    if (extractStart == 0L) {
      return System.currentTimeMillis();
    }
    return (System.currentTimeMillis() - extractStart * 1000L);
  }

  public static boolean getPublishLog() {
    return getBoolean(PUBLISH_JMETER_LOG, false);
  }

  public static String getPaymentType() {
    return Enums.PaymentType.values()[RandomUtils.nextInt(0, Enums.PaymentType.values().length)].name();
  }

  public static String getTenantToClone() {
    return getString(TENANT_TO_CLONE, EMPTY_STRING);
  }

  public static String getSystemAdminPassword() {
    return getString(SYSTEM_ADMIN_PASSWORD, "systemadmin");
  }

  public static String getSystemAdminUser() {
    return getString(SYSTEM_ADMIN_USER, "systemadmin@isima.io");
  }

  public static int getLoadPercent() {
    return getInt(LOAD_PERCENT, 20);
  }

  public static long[] getExponentialDistribution(int dataPointLength, int maxReqSize) {
    final long lamda = 10000;
    Random rng = new Random();
    ExponentialGenerator gen = new ExponentialGenerator(10, rng);
    boolean running = true;
    int idx = 0;
    long[] dataPoints = new long[dataPointLength];
    while (idx < dataPointLength) {
      long interval = Math.round((gen.nextValue() * lamda) % maxReqSize);
      if (idx > 0 && interval < dataPoints[idx - 1] / 2) {
        interval = dataPoints[idx - 1] / 2;
      }
      dataPoints[idx++] = interval;
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

    }
    return dataPoints;
  }


}
