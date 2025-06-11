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
package io.isima.bios.data.impl.storage;

import io.isima.bios.exceptions.ApplicationException;
import java.util.Properties;

/** CassTable properties interface. */
public interface CassTableProperties {

  /**
   * Get property value of the specified key.
   *
   * @param key Property key.
   * @return Property value.
   * @throws ApplicationException when an unexpected error happens.
   */
  String getProperty(String key) throws ApplicationException;

  /**
   * Get property value as long type.
   *
   * @param key Property key.
   * @param defaultValue Default value in case the stored value is missing or invalid.
   * @return Property value.
   * @throws ApplicationException when an unexpected error happens.
   */
  long getPropertyAsLong(String key, long defaultValue) throws ApplicationException;

  /**
   * Set a property.
   *
   * @param key Property key.
   * @param value Property value.
   * @throws ApplicationException when an unexpected error happens.
   */
  void setProperty(String key, String value) throws ApplicationException;

  /**
   * Add properties.
   *
   * @param properties Properties to add.
   * @throws ApplicationException when an unexpected error happens.
   */
  void addProperties(Properties properties) throws ApplicationException;

  /**
   * Erase all properties for the table from its property storage.
   *
   * @throws ApplicationException when an unexpected error happens.
   */
  default void eraseProperties() throws ApplicationException {}
}
