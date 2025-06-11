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

import com.fasterxml.jackson.core.JsonProcessingException;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.io.IOException;
import java.util.Properties;

/** This implementation is based on SharedProperties. */
public class DefaultCassTableProperties implements CassTableProperties {
  private static final String PREFIX = "table:";

  private final String propertyKey;

  public DefaultCassTableProperties(CassStream cassStream) {
    if (cassStream == null) {
      throw new IllegalArgumentException("cassStream and sharedProperties may not be null");
    }
    final StreamDesc streamDesc = cassStream.getStreamDesc();
    propertyKey =
        PREFIX
            + cassStream.getTenantName()
            + "."
            + streamDesc.getSchemaName()
            + "."
            + streamDesc.getSchemaVersion();
  }

  @Override
  public String getProperty(String key) throws ApplicationException {
    return getProperties().getProperty(key);
  }

  @Override
  public long getPropertyAsLong(String key, long defaultValue) throws ApplicationException {
    final String src = getProperty(key);
    if (src == null) {
      return defaultValue;
    }
    try {
      return Long.parseLong(src);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  @Override
  public void setProperty(String key, String value) throws ApplicationException {
    final Properties properties = getProperties();
    properties.setProperty(key, value);
    saveProperties(properties);
  }

  @Override
  public void addProperties(Properties properties) throws ApplicationException {
    final Properties existing = getProperties();
    existing.putAll(properties);
    saveProperties(existing);
  }

  @Override
  public void eraseProperties() {
    // TODO(TFOS-1631): Implement the method
  }

  private final Properties getProperties() throws ApplicationException {
    final String value = BiosModules.getSharedProperties().getProperty(propertyKey);
    if (value == null) {
      return new Properties();
    }
    try {
      return TfosObjectMapperProvider.get().readValue(value, Properties.class);
    } catch (IOException e) {
      return new Properties();
    }
  }

  private void saveProperties(Properties properties) throws ApplicationException {
    String value;
    try {
      value = TfosObjectMapperProvider.get().writeValueAsString(properties);
    } catch (JsonProcessingException e) {
      throw new ApplicationException("encoding property failed", e);
    }
    BiosModules.getSharedProperties().setProperty(propertyKey, value);
  }
}
