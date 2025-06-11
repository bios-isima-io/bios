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
package io.isima.bios.admin.v1;

import io.isima.bios.models.Duplicatable;
import io.isima.bios.models.v1.AttributeDesc;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import lombok.ToString;

@ToString
public class StreamConversion extends Duplicatable {
  public enum ConversionType {
    NO_CHANGE,
    ADD,
    CONVERT
  }

  @ToString
  public static class AttributeConversion {
    private final AttributeDesc oldDesc;
    private final AttributeDesc newDesc;
    private final ConversionType conversionType;

    protected AttributeConversion(
        ConversionType conversionType, AttributeDesc oldDesc, AttributeDesc newDesc) {
      if (conversionType == null) {
        throw new IllegalArgumentException("conversionType may not be null");
      }
      if (newDesc == null) {
        throw new IllegalArgumentException("newDesc may not be null");
      }
      this.oldDesc = oldDesc;
      this.newDesc = newDesc;
      this.conversionType = conversionType;
    }

    public ConversionType getConversionType() {
      return conversionType;
    }

    public String getNewName() {
      return newDesc.getName();
    }

    public Object getDefaultValue() {
      return newDesc.getInternalDefaultValue();
    }

    public AttributeDesc getNewDesc() {
      return newDesc;
    }

    public AttributeDesc getOldDesc() {
      return oldDesc;
    }

    @Override
    public boolean equals(Object target) {
      if (target == null) {
        return false;
      }
      if (this == target) {
        return true;
      }
      if (!(target instanceof AttributeConversion)) {
        return false;
      }
      final AttributeConversion that = (AttributeConversion) target;
      return memberEquals(newDesc, that.newDesc)
          && memberEquals(conversionType, that.conversionType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(newDesc, conversionType);
    }
  }

  private Map<String, AttributeConversion> conversionTable;

  public StreamConversion() {
    conversionTable = new LinkedHashMap<>();
  }

  @Override
  public StreamConversion duplicate() {
    final StreamConversion clone = new StreamConversion();
    conversionTable.forEach((key, value) -> clone.conversionTable.put(key, value));
    return clone;
  }

  public void addAttributeConversion(AttributeConversion conv) {
    conversionTable.put(conv.getNewName(), conv);
  }

  public StreamConversion addAttributeNoChange(AttributeDesc oldDesc, AttributeDesc newDesc) {
    if (oldDesc == null) {
      throw new IllegalArgumentException("Parameter oldDesc may not be null");
    }
    conversionTable.put(
        newDesc.getName().toLowerCase(),
        new AttributeConversion(ConversionType.NO_CHANGE, oldDesc, newDesc));
    return this;
  }

  public StreamConversion addAttributeAdd(AttributeDesc newDesc) {
    conversionTable.put(
        newDesc.getName().toLowerCase(),
        new AttributeConversion(ConversionType.ADD, null, newDesc));
    return this;
  }

  public StreamConversion addAttributeConvert(AttributeDesc oldDesc, AttributeDesc newDesc) {
    if (oldDesc == null) {
      throw new IllegalArgumentException("Parameter oldDesc may not be null");
    }
    conversionTable.put(
        newDesc.getName().toLowerCase(),
        new AttributeConversion(ConversionType.CONVERT, oldDesc, newDesc));
    return this;
  }

  public AttributeConversion getAttributeConversion(String name) {
    return conversionTable.get(name.toLowerCase());
  }

  public boolean hasChange() {
    for (Map.Entry<String, AttributeConversion> entry : conversionTable.entrySet()) {
      if (entry.getValue().conversionType != ConversionType.NO_CHANGE) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean equals(Object target) {
    if (target == null) {
      return false;
    }
    if (this == target) {
      return true;
    }
    if (!(target instanceof StreamConversion)) {
      return false;
    }
    final StreamConversion that = (StreamConversion) target;
    return memberEquals(conversionTable, that.conversionTable);
  }

  @Override
  public int hashCode() {
    return Objects.hash(conversionTable);
  }
}
