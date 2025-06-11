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
package io.isima.bios.integrations;

import io.isima.bios.errors.exception.InvalidConfigurationException;
import io.isima.bios.errors.exception.NotImplementedException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.integrations.validator.IntegrationError;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.ImportSourceConfig;
import io.isima.bios.models.ImportSourceSchema;
import io.isima.bios.models.SourceAttributeSchema;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class SourceSchemaFinder {

  public static SourceSchemaFinder newFinder(String tenantName, ImportSourceConfig config)
      throws NotImplementedException {
    if (config == null) {
      throw new IllegalArgumentException("import source config object must not be null");
    }
    switch (config.getType()) {
      case KAFKA:
        return new KafkaSourceSchemaFinder(tenantName, config);
      case MYSQL:
      case MYSQL_PULL:
        return new MySqlSourceSchemaFinder(tenantName, config);
      case MONGODB:
        return new MongoSourceSchemaFinder(tenantName, config);
      case POSTGRES:
        return new PostgresSourceSchemaFinder(tenantName, config);
      default:
        throw new NotImplementedException(
            "Schema finder for source type "
                + config.getType().stringify()
                + " is not implemented yet");
    }
  }

  protected final String tenantName;
  protected final ImportSourceConfig config;
  protected final String errorContext;

  protected SourceSchemaFinder(String tenantName, ImportSourceConfig config) {
    this.tenantName = Objects.requireNonNull(tenantName);
    this.config = Objects.requireNonNull(config);
    errorContext =
        String.format(
            "tenant=%s, importSource=%s (%s)",
            tenantName, config.getImportSourceName(), config.getImportSourceId());
  }

  public abstract ImportSourceSchema find(Integer timeoutSeconds)
      throws TfosException, ApplicationException;

  /**
   * Parses a JASON object into list of source attribute schemas.
   *
   * <p>The method does not parse the input when the object consists of a single value. The method
   * returns false in the case of skipping the parsing.
   *
   * @param object Input JSON object
   * @param attributes List of attributes schemas to collect
   * @return True if the object does not consist of a single value, otherwise false
   * @throws TfosException thrown to indicate that the parser could not make attribute schemas.
   */
  protected static boolean parseJsonObject(Object object, List<SourceAttributeSchema> attributes)
      throws TfosException {
    if (object instanceof Map) {
      parseJsonMap("", (Map<?, ?>) object, attributes, true);
      return true;
    } else if (object instanceof List) {
      try {
        parseJsonList("*/", (List<?>) object, attributes);
        return true;
      } catch (UnableToFlattenException e) {
        return false;
      }
    }
    return false;
  }

  private static void parseJsonMap(
      String path, Map<?, ?> map, List<SourceAttributeSchema> attributes, boolean allowNested)
      throws TfosException {
    for (var entry : map.entrySet()) {
      final String name = entry.getKey().toString();
      final Object value = entry.getValue();
      AttributeType type = null;
      String originalType = null;
      if (value instanceof Map) {
        if (allowNested) {
          parseJsonMap(path + name + "/", (Map<?, ?>) value, attributes, true);
        } else {
          type = AttributeType.STRING;
          originalType = "object";
        }
      } else if (value instanceof List) {
        if (allowNested) {
          try {
            parseJsonList(path + name + "/*/", (List<?>) value, attributes);
          } catch (UnableToFlattenException e) {
            type = AttributeType.STRING;
            originalType = "list";
          }
        } else {
          type = AttributeType.STRING;
          originalType = "list";
        }
      } else if (value instanceof String || value == null) {
        type = AttributeType.STRING;
        originalType = value != null ? "string" : "null";
      } else if (value instanceof Boolean) {
        type = AttributeType.BOOLEAN;
        originalType = "boolean";
      } else if (value instanceof Long) {
        type = AttributeType.INTEGER;
        originalType = "number";
      } else if (value instanceof Double) {
        type = AttributeType.DECIMAL;
        originalType = "number";
      } else {
        throw new TfosException(
            IntegrationError.IMPORT_SOURCE_UNABLE_TO_PARSE,
            "Could not understand the input data structure");
      }
      if (type != null) {
        final var attributeSchema = new SourceAttributeSchema();
        attributeSchema.setSourceAttributeName(path + name);
        attributeSchema.setSuggestedType(type);
        attributeSchema.setOriginalType(originalType);
        attributeSchema.setIsNullable(value == null);
        attributeSchema.setExampleValue(value);
        attributes.add(attributeSchema);
      }
    }
  }

  private static void parseJsonList(
      String path, List<?> list, List<SourceAttributeSchema> attributes)
      throws TfosException, UnableToFlattenException {
    final var combined = new LinkedHashMap<String, SourceAttributeSchema>();
    final var count = new HashMap<String, Integer>();
    boolean initialRound = true;
    for (Object entry : list) {
      if (!(entry instanceof Map)) {
        throw new UnableToFlattenException();
      }
      final var currentAttributes = new ArrayList<SourceAttributeSchema>();
      parseJsonMap(path, (Map<?, ?>) entry, currentAttributes, false);

      if (initialRound) {
        for (var attribute : currentAttributes) {
          final var name = attribute.getSourceAttributeName();
          combined.put(name, attribute);
          count.put(name, 1);
        }
      } else {
        for (var attribute : currentAttributes) {
          final var name = attribute.getSourceAttributeName();
          var existing = combined.get(name);
          if (existing == null) {
            combined.put(name, attribute);
            count.put(name, 1);
          } else {
            if (attribute.getIsNullable()) {
              existing.setIsNullable(true);
            } else {
              existing.setExampleValue(attribute.getExampleValue());
            }
            // If there's a mismatch in attribute type, use the generic String type
            if (attribute.getSuggestedType() != existing.getSuggestedType()) {
              existing.setSuggestedType(AttributeType.STRING);
              if (!existing.getOriginalType().contains(attribute.getOriginalType())) {
                existing.setOriginalType(
                    existing.getOriginalType() + ", " + attribute.getOriginalType());
              }
            }
            count.put(name, count.get(name) + 1);
          }
        }
      }
      initialRound = false;
    }

    for (var entry : combined.entrySet()) {
      final var attr = entry.getValue();
      if (count.get(entry.getKey()) < list.size()) {
        attr.setIsNullable(true);
      }
      attributes.add(attr);
    }
  }

  /**
   * Decodes a base64 string.
   *
   * @param src The source string
   * @param propertyName Property name of the source string
   * @throws InvalidConfigurationException thrown when an error encountered while decoding
   * @returns Decoded byte array
   */
  protected byte[] base64Decode(String src, String propertyName)
      throws InvalidConfigurationException {
    try {
      return Base64.getDecoder().decode(src);
    } catch (IllegalArgumentException e) {
      throw new InvalidConfigurationException(
          String.format(
              "Invalid value; %s, property=tls.truststoreContent, error=%s",
              errorContext, propertyName, e.getMessage()));
    }
  }

  static class UnableToFlattenException extends Exception {}
}
