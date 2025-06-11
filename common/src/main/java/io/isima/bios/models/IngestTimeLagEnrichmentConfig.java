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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public class IngestTimeLagEnrichmentConfig extends Duplicatable {
  @JsonProperty("ingestTimeLagName")
  @NotNull
  private String name;

  @JsonProperty("attribute")
  @NotNull
  private String attribute;

  @JsonProperty("as")
  @NotNull
  private String as;

  @JsonProperty("tags")
  private AttributeTags tags;

  private String fillInSerialized;

  @JsonIgnore private AttributeValueGeneric fillIn;

  public IngestTimeLagEnrichmentConfig(String name, String attribute) {
    this.name = name;
    this.attribute = attribute;
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

  @JsonProperty("fillIn")
  private Object getFillInOrSerialized() {
    if (fillIn != null) {
      return fillIn.asObject();
    } else {
      return fillInSerialized;
    }
  }

  @Override
  public Duplicatable duplicate() {
    final var duplicate = new IngestTimeLagEnrichmentConfig();
    duplicate.name = name;
    duplicate.attribute = attribute;
    duplicate.as = as;
    duplicate.tags = tags; // tags are not immutable but writing their copy constructor is too much
    duplicate.fillInSerialized = fillInSerialized;
    duplicate.fillIn = fillIn;
    return duplicate;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof IngestTimeLagEnrichmentConfig)) {
      return false;
    }
    final var that = (IngestTimeLagEnrichmentConfig) other;
    return Objects.equals(name, that.name)
        && Objects.equals(attribute, that.attribute)
        && Objects.equals(as, that.as)
        && Objects.equals(tags, that.tags)
        && Objects.equals(fillInSerialized, that.fillInSerialized)
        && Objects.equals(fillIn, that.fillIn);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, attribute, as, tags, fillInSerialized, fillIn);
  }
}
