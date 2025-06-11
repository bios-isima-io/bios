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
import java.util.Objects;

/** Schema for an enrichment attribute. */
@JsonInclude(Include.NON_NULL)
public class EnrichmentAttribute {
  /** Context attribute name. */
  @JsonProperty("attributeName")
  private String attributeName;

  /**
   * Attribute name in signal used for this enriched attribute. {@link #attributeName} is used when
   * this property is omitted.
   */
  @JsonProperty("as")
  private String as;

  /**
   * Value used when the lookup entry is missing and missingAttributePolicy is STORE_FILLIN_VALUE.
   */
  // TODO(Naoki): Generalize the serialized data type when we support more than JSON
  private String fillInSerialized;

  /*
   * FillIn value of internal type, actually used for filling in.
   */
  @JsonIgnore private AttributeValueGeneric fillIn;

  public EnrichmentAttribute() {}

  public EnrichmentAttribute(String attributeName) {
    this.attributeName = attributeName;
  }

  public EnrichmentAttribute(String attributeName, String as) {
    this.attributeName = attributeName;
    this.as = as;
  }

  public String getAttributeName() {
    return attributeName;
  }

  public void setAttributeName(String attributeName) {
    this.attributeName = attributeName;
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
  public String toString() {
    StringBuilder sb = new StringBuilder("{");
    sb.append("attributeName=").append(attributeName);
    if (as != null) {
      sb.append(", as=").append(as);
    }
    if (fillIn != null) {
      sb.append(", fillIn=").append(fillIn);
    }
    if (fillInSerialized != null) {
      sb.append(", fillIn(conf)=").append(fillInSerialized);
    }
    return sb.append("}").toString();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof EnrichmentAttribute)) {
      return false;
    }
    final var that = (EnrichmentAttribute) other;
    return Objects.equals(attributeName, that.attributeName)
        && Objects.equals(as, that.as)
        && Objects.equals(fillInSerialized, that.fillInSerialized)
        && Objects.equals(fillIn, that.fillIn);
  }

  @Override
  public int hashCode() {
    return Objects.hash(attributeName, as, fillInSerialized, fillIn);
  }
}
