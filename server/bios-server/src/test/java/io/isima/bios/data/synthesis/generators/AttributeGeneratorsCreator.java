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
package io.isima.bios.data.synthesis.generators;

import io.isima.bios.errors.GenericError;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.BiosStreamConfig;
import io.isima.bios.models.SignalConfig;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Creator of a collection of attribute generators for a signal/context. */
public class AttributeGeneratorsCreator {

  public static AttributeGeneratorsCreator getCreator(
      String tenantName, BiosStreamConfig biosStreamConfig) {
    return new AttributeGeneratorsCreator(tenantName, biosStreamConfig);
  }

  private final String tenantName;
  private final BiosStreamConfig biosStreamConfig;
  private GeneratorResolver externalResolver;

  private AttributeGeneratorsCreator(String tenantName, BiosStreamConfig biosStreamConfig) {
    Objects.requireNonNull(biosStreamConfig);
    Objects.requireNonNull(biosStreamConfig.getAttributes());
    this.tenantName = tenantName;
    this.biosStreamConfig = biosStreamConfig;
  }

  /**
   * Sets an external generator resolver to override default generators.
   *
   * @param resolver The external resolver
   * @return Self
   */
  public AttributeGeneratorsCreator externalResolver(GeneratorResolver resolver) {
    this.externalResolver = resolver;
    return this;
  }

  /**
   * Creates a collection of attribute generators.
   *
   * @return The collection of attribute generators as a map of attribute name and generator.
   * @throws TfosException thrown to indicate that the method cannot create a record generator for
   *     the specified stream.
   * @throws ApplicationException thrown when an unexpected problem happens.
   */
  public Map<String, AttributeGenerator<?>> create() throws TfosException, ApplicationException {

    final var generators = new LinkedHashMap<String, AttributeGenerator<?>>();

    if (externalResolver != null) {
      generators.putAll(externalResolver.resolveAttributeGenerators(tenantName, biosStreamConfig));
    }

    for (var attribute : biosStreamConfig.getAttributes()) {
      if (!generators.containsKey(attribute.getName())) {
        generators.put(attribute.getName(), createGenericGenerator(biosStreamConfig, attribute));
      }
    }
    return generators;
  }

  /**
   * Creates a generic generator for an attribute.
   *
   * @param attribute Attribute configuration
   * @return The generic generator for the attribute.
   * @throws TfosException Thrown to indicate that the data synthesizer does not support the
   *     specified attribute.
   */
  private AttributeGenerator<?> createGenericGenerator(
      BiosStreamConfig streamConfig, AttributeConfig attribute) throws TfosException {
    switch (attribute.getType()) {
      case STRING:
        if (attribute.getAllowedValues() == null) {
          return new GenericStringGenerator(32);
        } else if (streamConfig instanceof SignalConfig) {
          return new InternalValueStringEnumGenerator(attribute.getAllowedValues());
        } else {
          return new GenericStringEnumGenerator(attribute.getAllowedValues());
        }
      case INTEGER:
        return new GenericIntegerGenerator(
            SynthesisParameters.INTEGER_MIN, SynthesisParameters.INTEGER_MAX);
      case DECIMAL:
        return new GenericDecimalGenerator(
            SynthesisParameters.DECIMAL_MIN, SynthesisParameters.DECIMAL_MAX);
      case BOOLEAN:
        return new GenericBooleanGenerator(SynthesisParameters.BOOLEAN_TRUE_RATIO);
      case BLOB:
        return new GenericBlobGenerator();
      default:
        throw new TfosException(
            GenericError.OPERATION_UNEXECUTABLE,
            String.format(
                "Cannot synthesize attribute; tenant=%s, signal=%s, attribute=%s, type=%s",
                tenantName, biosStreamConfig.getName(), attribute.getName(), attribute.getType()));
    }
  }

  public String getTenantName() {
    return tenantName;
  }

  public BiosStreamConfig getStreamConfig() {
    return biosStreamConfig;
  }
}
