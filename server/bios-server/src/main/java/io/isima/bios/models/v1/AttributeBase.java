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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.isima.bios.models.Duplicatable;
import io.isima.bios.models.v1.validators.ValidatorConstants;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@ToString
@EqualsAndHashCode(callSuper = false)
public class AttributeBase extends Duplicatable {

  /** Attribute name. */
  @JsonProperty(value = "name")
  @NotNull
  @Size(min = 1, max = 40)
  @Pattern(regexp = ValidatorConstants.NAME_PATTERN)
  protected String name;

  /** Attribute type configured using configuration JSON. */
  @JsonProperty(value = "type")
  @NotNull
  protected InternalAttributeType attributeType;

  public AttributeBase() {}

  public AttributeBase(String name, InternalAttributeType attributeType) {
    this.name = name;
    this.attributeType = attributeType;
  }

  @Override
  public AttributeBase duplicate() {
    AttributeBase clone = new AttributeBase();
    clone.incorporate(this);
    return clone;
  }

  protected void incorporate(AttributeBase src) {
    name = src.name;
    attributeType = src.attributeType;
  }
}
