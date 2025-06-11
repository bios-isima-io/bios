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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.isima.bios.models.v1.AttributeDesc;
import java.util.ArrayList;
import java.util.List;

/**
 * Index table descriptor configuration.
 *
 * @author aj
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class IndexDesc {

  /**
   * Constructor that is meant be used by Jackson mapper to deserialize this class.
   *
   * @param indexNames List of names as value of property "attributes".
   */
  @JsonCreator
  public IndexDesc(@JsonProperty("attributes") List<String> indexNames) {
    if (indexNames != null) {
      attributes = new ArrayList<>();
      for (String name : indexNames) {
        AttributeDesc desc = new AttributeDesc();
        desc.setName(name);
        attributes.add(desc);
      }
    }
  }

  /** Index name. */
  @JsonIgnore private String name;

  /** Index attributes. */
  private List<AttributeDesc> attributes;

  /**
   * Returns name of Index table.
   *
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * Sets name of Index table.
   *
   * @param name the name to set
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Returns attributes of index table.
   *
   * @return the attributes
   */
  public List<AttributeDesc> getAttributes() {
    return attributes;
  }

  /**
   * Getter of property "attributes" used by Jackson.
   *
   * @return List of attribute names
   */
  @JsonGetter("attributes")
  public List<String> toJsonString() {
    List<String> values = new ArrayList<>();
    for (AttributeDesc desc : attributes) {
      values.add(desc.getName());
    }
    return values;
  }

  /**
   * Sets attributes of index table.
   *
   * @param attributes the attributes to set
   */
  public void setAttributes(List<AttributeDesc> attributes) {
    this.attributes = attributes;
  }
}
