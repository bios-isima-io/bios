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
package io.isima.bios.models.v1;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.isima.bios.errors.exception.InvalidValueSyntaxException;
import io.isima.bios.models.AttributeTags;
import io.isima.bios.models.MissingAttributePolicyV1;
import io.isima.bios.models.v1.validators.ValidAttributeDesc;
import java.util.ArrayList;
import java.util.List;
import javax.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/** Attribute descriptor. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
@ValidAttributeDesc
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class AttributeDesc extends AttributeBase {

  /** Behavior when value of the attribute is missing on ingestion/put. */
  private MissingAttributePolicyV1 missingValuePolicy;

  /** Default value, if any. */
  private Object defaultValue;

  /**
   * Default value of TFOS internal data type. This member is only for internal use and won't be
   * sent over the wire. The original input form is preserved in member defaultValue. The original
   * form is necessary for saving the config to DB and for providing AttributeDesc config to SDK.
   */
  @JsonIgnore private Object internalDefaultValue;

  private List<String> enumList;

  /** Minimum value, if any. */
  private Object minValue;

  /** Maximum value, if any. */
  private Object maxValue;

  @Getter @Setter private AttributeTags tags;
  @Getter @Setter @EqualsAndHashCode.Exclude private AttributeTags inferredTags;

  public AttributeDesc() {}

  @JsonCreator
  public static AttributeDesc deserialize(
      @JsonProperty("name") String name,
      @JsonProperty("type") @NotNull InternalAttributeType type,
      @JsonProperty("enum") List<Object> enumList,
      @JsonProperty("defaultValue") Object defaultValue,
      @JsonProperty("tags") AttributeTags tags,
      @JsonProperty("inferredTags") AttributeTags inferredTags)
      throws InvalidValueSyntaxException {
    AttributeDesc desc = new AttributeDesc();
    desc.setName(name);
    desc.setAttributeType(type);
    if (type == InternalAttributeType.ENUM) {
      if (enumList == null) {
        throw new InvalidValueSyntaxException(
            "Enum attribute type requires 'enum' entry list; attribute=" + name);
      }
      List<String> names = new ArrayList<>();
      for (int i = 0; i < enumList.size(); ++i) {
        Object entry = enumList.get(i);
        if (entry instanceof String) {
          final String normalized = ((String) entry).trim();
          if (normalized.isEmpty()) {
            throw new InvalidValueSyntaxException(
                String.format(
                    "Enum entry name may not be empty; attribute=%s, enum_index=%d", name, i));
          }
          names.add(normalized);
        } else {
          throw new InvalidValueSyntaxException(
              String.format(
                  "Enum entry name must be a string; attribute=%s, enum_index=%d, value=%s",
                  name, i, entry));
        }
      }
      desc.setEnum(names);
    }
    if (defaultValue != null) {
      desc.setInternalDefaultValue(Attributes.parseDefaultValue(defaultValue, desc));
      desc.setDefaultValue(defaultValue);
    }
    desc.setTags(tags);
    desc.setInferredTags(inferredTags);

    return desc;
  }

  @Override
  public AttributeDesc duplicate() {
    AttributeDesc clone = new AttributeDesc();
    clone.incorporate(this);
    return clone;
  }

  protected void incorporate(AttributeDesc src) {
    super.incorporate(src);
    missingValuePolicy = src.missingValuePolicy;
    defaultValue = src.defaultValue;
    internalDefaultValue = src.internalDefaultValue;
    enumList = src.enumList != null ? new ArrayList<>(src.enumList) : null;
    minValue = src.minValue;
    maxValue = src.maxValue;
    tags = src.tags;
    inferredTags = src.inferredTags;
  }

  /**
   * Constructor with a set of basic parameters.
   *
   * @param name Attribute name
   * @param attributeType Attribute type
   */
  public AttributeDesc(String name, InternalAttributeType attributeType) {
    this.name = name;
    this.attributeType = attributeType;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public InternalAttributeType getAttributeType() {
    return attributeType;
  }

  public void setAttributeType(InternalAttributeType attributeType) {
    this.attributeType = attributeType;
  }

  public MissingAttributePolicyV1 getMissingValuePolicy() {
    return missingValuePolicy;
  }

  public AttributeDesc setMissingValuePolicy(MissingAttributePolicyV1 policy) {
    this.missingValuePolicy = policy;
    return this;
  }

  public Object getDefaultValue() {
    return defaultValue;
  }

  public AttributeDesc setDefaultValue(Object defaultValue) {
    this.defaultValue = defaultValue;
    return this;
  }

  public Object getInternalDefaultValue() {
    return internalDefaultValue;
  }

  public AttributeDesc setInternalDefaultValue(Object internalDefaultValue) {
    this.internalDefaultValue = internalDefaultValue;
    return this;
  }

  public List<String> getEnum() {
    return enumList;
  }

  public AttributeDesc setEnum(List<String> enumList) {
    this.enumList = enumList;
    return this;
  }

  public Object getMinValue() {
    return minValue;
  }

  public void setMinValue(Object minValue) {
    this.minValue = minValue;
  }

  public Object getMaxValue() {
    return maxValue;
  }

  public void setMaxValue(Object maxValue) {
    this.maxValue = maxValue;
  }

  @JsonIgnore
  public AttributeTags getEffectiveTags() {
    return AttributeTags.getEffectiveTags(attributeType.getBiosAttributeType(), tags, inferredTags);
  }

  // Utilities //////////////////////////////////////////////////////

  /**
   * Finds any attribute type mismatch.
   *
   * @param test Counterpart AttributeDesc object to test.
   * @return null if attribute types match, otherwise reason for the mismatch
   */
  public String checkMismatch(AttributeDesc test) {
    if (attributeType != test.attributeType) {
      return "different attribute types";
    }
    if (attributeType == InternalAttributeType.ENUM && enumList != test.enumList) {
      if (enumList.size() != test.enumList.size()) {
        return "different number of enum entries";
      }
      for (int i = 0; i < enumList.size(); ++i) {
        if (!enumList.get(i).equals(test.enumList.get(i))) {
          return "enum entries differ at index " + i;
        }
      }
    }
    return null;
  }
}
