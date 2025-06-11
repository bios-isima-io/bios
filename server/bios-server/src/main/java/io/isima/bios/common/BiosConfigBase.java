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
package io.isima.bios.common;

import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BiosConfigBase {
  private static final Logger logger = LoggerFactory.getLogger(BiosConfigBase.class);

  private static BiosConfigBase instance;

  private Properties properties = System.getProperties();

  /**
   * TfosConfigBase is instantiated as a singleton.
   *
   * @return The instance.
   */
  public static BiosConfigBase getInstance() {
    if (instance == null) {
      instance = new BiosConfigBase();
    }
    return instance;
  }

  public static void setProperties(Properties properties) {
    getInstance().properties = properties;
  }

  /**
   * Generic method to get property as string.
   *
   * @param key the property key
   * @param defaultValue default value used in case the property with the specified key is missing.
   * @return property as a string.
   */
  public String getString(String key, String defaultValue) {
    String value = properties.getProperty(key);
    return value != null ? value.trim() : defaultValue;
  }

  /**
   * Method to get property or an environment variable.
   *
   * <p>If the value includes a string ${varname}, the method replaces the value by an environment
   * variable specified by varname.
   *
   * @param key the property key
   * @param defaultValue default value used in case the property with the specified key is missing.
   * @return property as a string.
   */
  public String getStringReplacedByEnv(String key, String defaultValue) {
    String value = properties.getProperty(key);
    if (value == null) {
      return defaultValue;
    }
    final var sb = new StringBuilder();
    int startIndex = 0;
    do {
      int nextStartIndex = value.indexOf("${", startIndex);
      if (nextStartIndex < 0 || nextStartIndex + 2 >= value.length()) {
        break;
      }
      sb.append(value.substring(startIndex, nextStartIndex));
      int endIndex = value.indexOf("}", nextStartIndex + 2);
      if (endIndex < 0) {
        break;
      }
      final String envVarName = value.substring(startIndex + 2, endIndex);
      if (!envVarName.isBlank()) {
        final var env = System.getenv(envVarName);
        if (env != null) {
          sb.append(env);
        }
      }
      startIndex = endIndex + 1;
    } while (startIndex < value.length());
    if (startIndex < value.length()) {
      sb.append(value.substring(startIndex));
    }
    return sb.toString();
  }

  /**
   * Generic method to get property as integer.
   *
   * @param key the property key
   * @param defaultValue default value used in case the property with the specified key is missing.
   * @return property as an integer.
   */
  public int getInt(String key, int defaultValue) {
    String value = properties.getProperty(key);
    if (value == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value.trim());
    } catch (NumberFormatException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Generic method to get property as integer.
   *
   * @param key the property key
   * @param defaultValue default value used in case the property with the specified key is missing.
   * @param minimumValue allowed minimum value
   * @param maximumValue allowed maximum value
   * @return property as an integer.
   */
  public int getInt(String key, int defaultValue, int minimumValue, int maximumValue) {
    String value = properties.getProperty(key);
    if (value == null) {
      return defaultValue;
    }
    try {
      final var intValue = Integer.parseInt(value.trim());
      if (intValue < minimumValue || intValue > maximumValue) {
        throw new RuntimeException(
            String.format(
                "Value of parameter %s is out of allowed range [%d : %d]: %d",
                key, minimumValue, maximumValue, intValue));
      }
      return intValue;
    } catch (NumberFormatException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Generic method to get property as double.
   *
   * @param key the property key
   * @param defaultValue default value used in case the property with the specified key is missing.
   * @return property as a double.
   */
  public double getDouble(String key, double defaultValue) {
    String value = properties.getProperty(key);
    if (value == null) {
      return defaultValue;
    }
    try {
      return Double.parseDouble(value.trim());
    } catch (NumberFormatException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Generic method to get property as long.
   *
   * @param key the property key
   * @param defaultValue default value used in case the property with the specified key is missing.
   * @return property as an integer.
   */
  public long getLong(String key, long defaultValue) {
    String value = properties.getProperty(key);
    if (value == null) {
      return defaultValue;
    }
    try {
      return Long.parseLong(value.trim());
    } catch (NumberFormatException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Generic method to get property as boolean.
   *
   * @param key the property key
   * @param defaultValue default value used in case the property with the specified key is missing.
   * @return property as an integer.
   */
  public boolean getBoolean(String key, boolean defaultValue) {
    String value = properties.getProperty(key);
    if (value == null) {
      return defaultValue;
    }
    return Boolean.parseBoolean(value.trim());
  }

  /**
   * Generic method to get property as list of string.
   *
   * @param key the property key
   * @param delimiter delimiter that splits the property
   * @param defaultValue default value used in case the property with the specified key is missing.
   * @return property as a list of string.
   */
  public String[] getStringArray(String key, String delimiter, String defaultValue) {
    String value = properties.getProperty(key);
    if (value == null) {
      value = defaultValue;
    }
    String[] values = value.split(delimiter);
    for (int i = 0; i < values.length; ++i) {
      values[i] = values[i].trim();
    }
    return values;
  }

  public String getPath(String key, String defaultValue) {
    final var value = getString(key, defaultValue);
    if (value == null) {
      return value;
    }
    final String biosHome = System.getProperty("user.dir");
    return value.replace("${BIOS_HOME}", biosHome);
  }
}
