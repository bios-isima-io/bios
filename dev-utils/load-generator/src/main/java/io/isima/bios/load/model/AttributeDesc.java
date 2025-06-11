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
package io.isima.bios.load.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.isima.bios.exception.ConfigException;
import java.util.ArrayList;
import java.util.List;

/**
 * Attribute descriptor.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class AttributeDesc {
  /**
   * Attribute name.
   */
  private String name;

  /**
   * Attribute type configured using configuration JSON.
   */
  @JsonProperty(value = "type")
  private AttributeType attributeType;

  /**
   * Attribute data range configured using configuration JSON.
   */
  @JsonProperty(value = "attrValType")
  private AttrValueType attrValType;

  /**
   * attribute regex pattern using configuration JSON.
   */
  @JsonProperty(value = "pattern")
  private String pattern;


  /**
   * Behavior when value of the attribute is missing on ingestion/put.
   */
  private Object missingAttributePolicy;

  /**
   * Default value, if any.
   */
  private Object defaultValue;

  /**
   * Default value of TFOS internal data type. This member is only for internal use and won't be
   * sent over the wire. The original input form is preserved in member defaultValue. The original
   * form is necessary for saving the config to DB and for providing AttributeDesc config to SDK.
   */
  @JsonIgnore
  private Object internalDefaultValue;

  private List<String> allowedValuesList;

  /**
   * Minimum value, if any.
   */
  private Object minValue;

  /**
   * Maximum value, if any.
   */
  private Object maxValue;

  public AttributeDesc() {}

  @JsonCreator
  public static AttributeDesc deserialize(@JsonProperty("attributeName") String name,
      @JsonProperty("type") AttributeType type,
      @JsonProperty("allowedValues") List<Object> enumList,
      @JsonProperty("default") Object defaultValue,
      @JsonProperty("attrValType") ValueType valType,
      @JsonProperty("range") String range,
      @JsonProperty("pattern") String pattern) throws Exception {
    AttributeDesc desc = new AttributeDesc();
    desc.setName(name);
    desc.setAttributeType(type);
    desc.setAttrValType(new AttrValueType(valType));
    if (range != null && !range.trim().isEmpty()) {
      String[] dataRange = range.split("-");
      if (null == dataRange || dataRange.length != 2) {
        throw new ConfigException(
            String.format("Invalid range for attribute %s, range: %s", name, range));
      }
      desc.setAttrValType(new AttrValueType(valType, Integer.valueOf(dataRange[0].trim()),
          Integer.valueOf(dataRange[1].trim())));
    }

    if (null != pattern && !pattern.trim().isEmpty()) {
      desc.setPattern(pattern);
    }

    if (enumList != null && enumList.size() > 0) {
      List<String> names = new ArrayList<String>();
      for (int i = 0; i < enumList.size(); ++i) {
        Object entry = enumList.get(i);
        if (entry instanceof String) {
          final String normalized = ((String) entry).trim();
          if (normalized.isEmpty()) {
            throw new Exception(String
                .format("Enum entry name may not be empty; attribute=%s, enum_index=%d", name, i));
          }
          names.add(normalized);
        } else {
          throw new Exception(
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
    return desc;
  }

  /**
   * Constructor with a set of basic parameters.
   *
   * @param name Attribute name
   * @param attributeType Attribute type
   */
  public AttributeDesc(String name, AttributeType attributeType) {
    this.name = name;
    this.attributeType = attributeType;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public AttributeType getAttributeType() {
    return attributeType;
  }

  public void setAttributeType(AttributeType attributeType) {
    this.attributeType = attributeType;
  }

  public Object getMissingAttributePolicy() {
    return missingAttributePolicy;
  }

  public AttributeDesc setMissingAttributePolicy(Object policy) {
    this.missingAttributePolicy = policy;
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

  public void setInternalDefaultValue(Object internalDefaultValue) {
    this.internalDefaultValue = internalDefaultValue;
  }

  public List<String> getEnum() {
    return allowedValuesList;
  }

  public AttributeDesc setEnum(List<String> enumList) {
    this.allowedValuesList = enumList;
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

  public AttrValueType getAttrValType() {
    return attrValType;
  }

  public void setAttrValType(AttrValueType attrValType) {
    this.attrValType = attrValType;
  }

  public String getPattern() {
    return pattern;
  }

  public void setPattern(String pattern) {
    this.pattern = pattern;
  }

  /**
   * Finds any attribute type mismatch.
   *
   * @param test Counterpart AttributeDesc object to test.
   * @return null if attribute types match, otherwise reason for the mismatch
   */
//  public String checkMismatch(AttributeDesc test) {
//    if (attributeType != test.attributeType) {
//      return "different attribute types";
//    }
//    if (attributeType == AttributeType.ENUM && allowedValuesList != test.allowedValuesList) {
//      if (allowedValuesList.size() != test.allowedValuesList.size()) {
//        return "different number of enum entries";
//      }
//      for (int i = 0; i < allowedValuesList.size(); ++i) {
//        if (!allowedValuesList.get(i).equals(test.allowedValuesList.get(i))) {
//          return "enum entries differ at index " + i;
//        }
//      }
//    }
//    return null;
//  }

}
