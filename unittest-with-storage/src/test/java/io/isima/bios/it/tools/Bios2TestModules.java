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
package io.isima.bios.it.tools;

import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.framework.BiosModules;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Bios2TestModules {

  private static final Map<String, String> savedProperties;
  private static final Map<String, String> specifiedProperties;

  private static Map<String, String> previousSharedProperties = null;

  static {
    savedProperties = new HashMap<>();
    specifiedProperties = new HashMap<>();
    System.getProperties()
        .forEach((key, value) -> savedProperties.put((String) key, (String) value));
  }

  /**
   * Method to start all TFOS modules but does not start maintenance tasks.
   *
   * <p>This method should be used only by tests.
   *
   * @param testClass Test class to be used to identify the invoker
   */
  public static void startModulesWithoutMaintenance(Class testClass) {
    previousSharedProperties =
        BiosModules.startModulesForTesting(
            makeInitialProperties(savedProperties), false, testClass, Map.of());
  }

  public static void startModulesWithoutMaintenance(String testClass) {
    previousSharedProperties =
        BiosModules.startModulesForTesting(
            makeInitialProperties(savedProperties), false, testClass, Map.of());
  }

  /**
   * Method to start all TFOS modules but does not start maintenance tasks.
   *
   * <p>This method should be used only by tests.
   *
   * @param runMaintenance Turn on/off the maintenance worker
   * @param testClass Test class to be used to identify the invoker
   * @param initialSharedProperties Shared properties to set before starting the test
   */
  public static void startModules(
      boolean runMaintenance, Class testClass, Map<String, String> initialSharedProperties) {
    previousSharedProperties =
        BiosModules.startModulesForTesting(
            makeInitialProperties(savedProperties),
            runMaintenance,
            testClass,
            initialSharedProperties);
  }

  public static void startModules(
      boolean runMaintenance, String testClass, Map<String, String> initialSharedProperties) {
    previousSharedProperties =
        BiosModules.startModulesForTesting(
            makeInitialProperties(savedProperties),
            runMaintenance,
            testClass,
            initialSharedProperties);
  }

  /**
   * Set a property before starting modules.
   *
   * <p>The purpose of the method is to configure the modules for testing.
   *
   * <p>The properties are reverted after modules are terminated by {@link #shutdown} method.
   *
   * @param key Property key.
   * @param value Property value.
   * @throws IllegalArgumentException when a null key is specified.
   * @throws IllegalStateException when this method is called after {@link #startModules} or {@link
   *     #startModulesWithoutMaintenance}.
   */
  public static void setProperty(String key, String value) {
    if (key == null) {
      throw new IllegalArgumentException("property key may not be null");
    }
    if (BiosModules.isStarted()) {
      throw new IllegalStateException(
          "Modules have started already. Call this method before startup or after shutdown.");
    }
    savedProperties.put(key, System.getProperty(key));
    specifiedProperties.put(key, value);
  }

  public static void clearSharedProperties(Collection<String> propertyNames)
      throws ApplicationException {
    for (var key : propertyNames) {
      BiosModules.getSharedProperties().setProperty(key, "");
    }
  }

  /** Shutdown modules and revert test properties. */
  public static void shutdown() {
    if (previousSharedProperties != null) {
      try {
        clearSharedProperties(previousSharedProperties.keySet());
      } catch (ApplicationException e) {
        throw new RuntimeException(e);
      }
      previousSharedProperties = null;
    }
    BiosModules.shutdown();
    BiosModules.setTestMode(false);
    savedProperties.forEach((key, value) -> TestUtils.revertProperty(key, value));
    savedProperties.clear();
    specifiedProperties.clear();
  }

  private static Properties makeInitialProperties(Map<String, String> savedProperties) {
    final var properties = loadProperties("server.options");
    properties.setProperty("io.isima.bios.server.io.threads", "2");
    properties.setProperty("io.isima.bios.server.digestor.threads", "1");
    return properties;
  }

  private static Properties loadProperties(String fileName) {
    Properties properties = new Properties();

    try (InputStream input =
        Bios2TestModules.class.getClassLoader().getResourceAsStream(fileName)) {
      if (input == null) {
        throw new IOException("Unable to find properties file: " + fileName);
      }
      properties.load(input);
    } catch (IOException e) {
      e.printStackTrace();
    }

    specifiedProperties.forEach((key, value) -> properties.setProperty(key, value));

    return properties;
  }
}
