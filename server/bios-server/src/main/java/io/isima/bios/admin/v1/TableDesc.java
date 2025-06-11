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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.isima.bios.models.v1.AttributeDesc;

/**
 * Table descriptor.
 *
 * @author aj
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TableDesc {

  /** Name of table. */
  private String name;

  /** Load table. */
  private boolean load;

  /** Save table. */
  private boolean save;

  /** Table key value pairs. */
  private AttributeDesc[] keys;

  /** Table key value pairs. */
  private AttributeDesc[] values;

  /**
   * Return name of table.
   *
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * Set the name of the table.
   *
   * @param name the name to set
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Returns whether load is set to true or false.
   *
   * @return the load
   */
  public boolean isLoad() {
    return load;
  }

  /**
   * Sets the load table to true or false.
   *
   * @param load the load to set
   */
  public void setLoad(boolean load) {
    this.load = load;
  }

  /**
   * Returns whether save is set to true or false.
   *
   * @return the save
   */
  public boolean isSave() {
    return save;
  }

  /**
   * Sets the save table to true or false.
   *
   * @param save the save to set
   */
  public void setSave(boolean save) {
    this.save = save;
  }

  /**
   * Return keys for key-value pairs stored in table.
   *
   * @return the keys
   */
  public AttributeDesc[] getKeys() {
    return keys;
  }

  /**
   * Set keys for key-value pairs to store in table.
   *
   * @param keys the keys to set
   */
  public void setKeys(AttributeDesc[] keys) {
    this.keys = keys;
  }

  /**
   * Returns values for key-value pairs stored in table.
   *
   * @return the values
   */
  public AttributeDesc[] getValues() {
    return values;
  }

  /**
   * Set values for key-value pairs to store in table.
   *
   * @param values the values to set
   */
  public void setValues(AttributeDesc[] values) {
    this.values = values;
  }
}
