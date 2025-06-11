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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;
import lombok.ToString;

/**
 * Schema for an enriched attribute including source and value, which may include some calculations.
 */
@JsonInclude(Include.NON_NULL)
@ToString
public class EnrichedAttribute extends Duplicatable {
  /** Value of the attribute in the format "ContextName.AttributeName". */
  @JsonProperty("value")
  private String value;

  /**
   * From among a list of values (in the same format as {@link #value}), pick the first one that is
   * present.
   */
  @JsonProperty("valuePickFirst")
  private List<String> valuePickFirst;

  /**
   * Attribute name in parent stream used for this enriched attribute. The AttributeName part of
   * {@link #value} is used when this property is omitted. If value is not present (e.g. if
   * valuePickFirst is used instead), then this property is required.
   */
  @JsonProperty("as")
  private String as;

  /** Value used when the lookup entry is missing and missingLookupPolicy is STORE_FILLIN_VALUE. */
  // TODO(Naoki): Generalize the serialized data type when we support more than JSON
  private String fillInSerialized;

  /*
   * FillIn value of internal type, actually used for filling in.
   */
  @JsonIgnore private AttributeValueGeneric fillIn;

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public List<String> getValuePickFirst() {
    return valuePickFirst;
  }

  public void setValuePickFirst(List<String> valuePickFirst) {
    this.valuePickFirst = valuePickFirst;
  }

  public String getAs() {
    return as;
  }

  public void setAs(String as) {
    this.as = as;
  }

  /**
   * Get the value used when the lookup entry is missing and missingAttributePolicy is
   * STORE_FILLIN_VALUE.
   *
   * <p>This method is used for the interface to JSON deserializer.
   *
   * @return The fill-in value as serialized data element
   */
  // TODO(Naoki): Find better way to send default.
  public String getFillInSerialized() {
    return fillInSerialized;
  }

  /**
   * Set the value used when the lookup entry is missing and missingAttributePolicy is
   * STORE_FILLIN_VALUE.
   *
   * <p>This method is used for the interface to JSON serializer, but not for actually filling-in.
   * The type of the fillIn value is unknown when the control plain API specifies this attribute.
   * Only Admin component can resolve the type by looking up the corresponding context. So we
   * receive the configuration with this method, and convert it to the proper type later in the
   * Admin component.
   *
   * @param fillIn FillIn config description as a serialized data element.
   */
  @JsonProperty("fillIn")
  void setFillInSerialized(String fillIn) {
    this.fillInSerialized = fillIn;
  }

  /**
   * Get fill-in value as the internal type.
   *
   * @return The fill-in value as the internal type.
   */
  public AttributeValueGeneric getFillIn() {
    return fillIn;
  }

  @JsonProperty("fillIn")
  private Object getFillInOrSerialized() {
    if (fillIn != null) {
      return fillIn.asObject();
    } else {
      return fillInSerialized;
    }
  }

  /**
   * Set the fill-in value as the internal type.
   *
   * <p>This methos is used for converting the serialized
   *
   * @param fillIn The fill-in value as the internal type.
   */
  public void setFillIn(AttributeValueGeneric fillIn) {
    this.fillIn = fillIn;
  }

  @Override
  public EnrichedAttribute duplicate() {
    EnrichedAttribute clone = new EnrichedAttribute();
    clone.value = value;
    clone.valuePickFirst = valuePickFirst;
    clone.as = as;
    clone.fillInSerialized = fillInSerialized;
    clone.fillIn = fillIn;
    return clone;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof EnrichedAttribute)) {
      return false;
    }

    final EnrichedAttribute test = (EnrichedAttribute) o;
    if (!(value == null ? test.value == null : value.equalsIgnoreCase(test.value))) {
      return false;
    }
    if (!(valuePickFirst == null
        ? test.valuePickFirst == null
        : valuePickFirst.equals(test.valuePickFirst))) {
      return false;
    }
    if (!(as == null ? test.as == null : as.equals(test.as))) {
      return false;
    }
    if (!(fillInSerialized == null
        ? test.fillInSerialized == null
        : fillInSerialized.equalsIgnoreCase(test.fillInSerialized))) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, valuePickFirst, as, fillInSerialized);
  }
}
