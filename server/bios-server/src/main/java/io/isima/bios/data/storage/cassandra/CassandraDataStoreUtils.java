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
package io.isima.bios.data.storage.cassandra;

import io.isima.bios.models.AttributeType;
import java.util.UUID;

/** Collection of utility methods for manipulating Cassandra DB. */
public class CassandraDataStoreUtils {

  /**
   * Method to generate a keyspace name from tenant name and version.
   *
   * @param tenantName Tenant name
   * @param version Tenant version
   * @return Keyspace name
   */
  public static String generateKeyspaceName(String tenantName, Long version) {
    return CassandraConstants.KEYSPACE_DATA_PREFIX + rotateEntityId(tenantName, version);
  }

  /**
   * Method to generate the table name from type, name and version.
   *
   * @param prefix Table prefix
   * @param name Name of the stream
   * @param version Version of the stream
   * @return Generated name of the table
   */
  public static String generateTableName(String prefix, String name, Long version) {
    return prefix + rotateEntityId(name, version);
  }

  /**
   * Common algorithm used for creating a keyspace or table name.
   *
   * <p>The method takes name and version as inputs, converts the name to lower case, joins the name
   * and version with a dot ('.') in between, generate an v3 UUID using the string, and represent it
   * as a hex numeric string (without hyphen).
   *
   * <p>e.g. Isima, 1602776720315 -> isima.1602776720315 -> 64fbbae171403b75bd815191cd5e24dc
   *
   * @param name Entity name
   * @param version Entity version
   * @return Rotated entity name
   */
  public static String rotateEntityId(String name, Long version) {
    final String source = name.toLowerCase() + "." + version;
    return (UUID.nameUUIDFromBytes(source.getBytes()).toString()).replaceAll("-", "");
  }

  /**
   * Converts BIOS attribute type to Cassandra data type name.
   *
   * <p>Mainly used for creating tables.
   *
   * @param attributeType BIOS attribute type
   * @param isEnum Flag to indicate whether the attribute is enum type.
   * @return Corresponding Cassandra data type name
   */
  public static String valueType2CassType(AttributeType attributeType, boolean isEnum) {
    if (isEnum) {
      return CassandraConstants.TYPE_INT;
    }
    switch (attributeType) {
      case STRING:
        return CassandraConstants.TYPE_TEXT;
      case INTEGER:
        return CassandraConstants.TYPE_LONG;
      case DECIMAL:
        return CassandraConstants.TYPE_DOUBLE;
      case BOOLEAN:
        return CassandraConstants.TYPE_BOOLEAN;
      case BLOB:
        return CassandraConstants.TYPE_BLOB;
      default:
        throw new UnsupportedOperationException("Unknown attribute type: " + attributeType);
    }
  }
}
