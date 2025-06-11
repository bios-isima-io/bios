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
package io.isima.bios.models;

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Base class of BIOS stream config object builder.
 *
 * @param <T> Stream config object type.
 * @param <BuilderT> Builder type.
 */
abstract class StreamConfigBuilder<T, BuilderT extends StreamConfigBuilder<T, BuilderT>> {

  protected final StreamDesc streamDesc;
  protected final String streamName;
  private final BuilderT builderObject;
  protected boolean least;
  protected boolean includeInferredTags;

  public StreamConfigBuilder(StreamDesc streamDesc) {
    this.streamDesc = streamDesc;
    this.streamName = streamDesc.getName();
    this.least = false;
    this.includeInferredTags = false;
    this.builderObject = (BuilderT) this;
  }

  /**
   * Have the builder build the least set of attributes.
   *
   * @param least flat to do least build
   * @return Self
   */
  public BuilderT least(boolean least) {
    this.least = least;
    return builderObject;
  }

  public BuilderT includeInferredTags(boolean includeInferredTags) {
    this.includeInferredTags = includeInferredTags;
    return builderObject;
  }

  /**
   * Run the build.
   *
   * @return Built BIOS config.
   * @throws InvalidRequestException thrown to indicate that the source TFOS stream config is not
   *     convertible to a BIOS config.
   */
  public final T build() throws InvalidRequestException {
    if (streamDesc != null) {
      return buildFromStreamDesc();
    } else {
      throw new IllegalStateException("StreamDesc is not set up properly");
    }
  }

  protected abstract String streamType();

  /**
   * Implement the TFOS StreamDesc to BIOS Config conversion here.
   *
   * @return Built BIOS config.
   * @throws InvalidRequestException thrown to indicate that the source TFOS stream config is not
   *     convertible to a BIOS config.
   */
  protected abstract T buildFromStreamDesc() throws InvalidRequestException;

  protected final MissingAttributePolicy translateMvp(MissingAttributePolicyV1 tfosPolicy) {
    if (tfosPolicy == null) {
      return null;
    }
    switch (tfosPolicy) {
      case STRICT:
        return MissingAttributePolicy.REJECT;
      case USE_DEFAULT:
        return MissingAttributePolicy.STORE_DEFAULT_VALUE;
      default:
        return null;
    }
  }

  protected final MissingLookupPolicy translateMlp(MissingAttributePolicyV1 tfosPolicy) {
    if (tfosPolicy == null) {
      return null;
    }
    switch (tfosPolicy) {
      case STRICT:
        return MissingLookupPolicy.REJECT;
      case USE_DEFAULT:
        return MissingLookupPolicy.STORE_FILL_IN_VALUE;
      case FAIL_PARENT_LOOKUP:
        return MissingLookupPolicy.FAIL_PARENT_LOOKUP;
      default:
        return null;
    }
  }

  protected final List<AttributeConfig> translateAttributes(
      List<AttributeDesc> tfosAttrs, MissingAttributePolicyV1 globalMvp)
      throws InvalidRequestException {
    if (tfosAttrs == null) {
      return null;
    }
    var biosAttrs = new ArrayList<AttributeConfig>();
    for (var tfosAttr : tfosAttrs) {
      biosAttrs.add(translateAttribute(tfosAttr, globalMvp));
    }
    return biosAttrs;
  }

  protected final AttributeConfig translateAttribute(
      AttributeDesc tfosAttr, MissingAttributePolicyV1 globalMvp) throws InvalidRequestException {
    if (tfosAttr == null) {
      return null;
    }
    final var biosAttr = new AttributeConfig();
    biosAttr.setName(tfosAttr.getName());
    switch (tfosAttr.getAttributeType()) {
      case STRING:
      case BOOLEAN:
      case LONG:
      case DOUBLE:
      case BLOB:
        biosAttr.setType(tfosAttr.getAttributeType().getBiosAttributeType());
        break;
      case ENUM:
        biosAttr.setType(io.isima.bios.models.AttributeType.STRING);
        if (tfosAttr.getEnum() != null) {
          biosAttr.setAllowedValues(
              tfosAttr.getEnum().stream()
                  .map(
                      entry ->
                          new AttributeValueGeneric(
                              entry, io.isima.bios.models.AttributeType.STRING))
                  .collect(Collectors.toList()));
        }
        break;
      default:
        throw new InvalidRequestException(
            String.format(
                "Unable to retrieve %s config, unsupported internal attribute type;"
                    + " %s=%s, attribute=%s, type=%s",
                streamType(),
                streamType(),
                streamName,
                tfosAttr.getName(),
                tfosAttr.getAttributeType()));
    }
    final var mvp = tfosAttr.getMissingValuePolicy();
    if (mvp != globalMvp) {
      biosAttr.setMissingAttributePolicy(translateMvp(mvp));
    }
    // TODO(BIOS-4419): Fix this mess.
    // The problem here is that defaultValue for a blob attribute should come as byte[]
    // and it is sometimes string. internalDefaultValue is ByteBuffer consistently, but
    // you have to copy the content to retrieve byte array in it.
    final Object defaultValue = tfosAttr.getDefaultValue();
    if (defaultValue != null) {
      final AttributeValueGeneric attributeValue;
      if (tfosAttr.getAttributeType() == InternalAttributeType.BLOB) {
        final var internalDefaultValue = (ByteBuffer) tfosAttr.getInternalDefaultValue();
        final byte[] blobValue = new byte[internalDefaultValue.limit()];
        internalDefaultValue.get(blobValue);
        internalDefaultValue.flip();
        attributeValue = new AttributeValueGeneric(blobValue, biosAttr.getType());
      } else {
        attributeValue = new AttributeValueGeneric(defaultValue.toString(), biosAttr.getType());
      }
      biosAttr.setDefaultValue(attributeValue);
    }

    biosAttr.setTags(tfosAttr.getTags());
    if (includeInferredTags) {
      biosAttr.setInferredTags(tfosAttr.getInferredTags());
    }

    return biosAttr;
  }
}
