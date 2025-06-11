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
import io.isima.bios.models.Duplicatable;
import io.isima.bios.models.MissingAttributePolicyV1;
import java.util.ArrayList;
import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Preprocessor descriptor which applies rule actions based on certain conditions to the incoming
 * stream.
 *
 * @author aj
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@ToString
public class PreprocessDesc extends Duplicatable {

  /** Name of preprocessor. */
  @NotNull
  @Size(min = 1, max = 255)
  private String name;

  /**
   * Foreign key attribute name, one dimensional.
   *
   * @deprecated Use {@link #foreignKey}
   */
  @NotNull
  @Size(min = 1, max = 40)
  @Deprecated
  private String condition;

  /** Foreign key attribute names. */
  @Getter @Setter private List<String> foreignKey;

  /** Policy to handle when lookup entry is missing. */
  @NotNull private MissingAttributePolicyV1 missingLookupPolicy;

  /** One or more preprocesssor rule actions. */
  @NotNull @Valid private List<ActionDesc> actions;

  /** Default constructor. */
  public PreprocessDesc() {
    actions = new ArrayList<>();
  }

  /**
   * Constructor with name.
   *
   * @param name Preprocessor name.
   */
  public PreprocessDesc(String name) {
    this.name = name;
    actions = new ArrayList<>();
  }

  /**
   * Method to make a clone of the instance.
   *
   * @return duplicated instance.
   */
  @Override
  public PreprocessDesc duplicate() {
    PreprocessDesc clone = new PreprocessDesc(name);
    clone.condition = condition;
    if (foreignKey != null) {
      clone.foreignKey = List.copyOf(foreignKey);
    }
    clone.actions = copyList(actions);
    clone.missingLookupPolicy = missingLookupPolicy;
    return clone;
  }

  /**
   * Returns name of the preprocessor.
   *
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * Sets name of the preprocessor.
   *
   * @param name the name to set
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Returns the condition on which rule actions will be applied.
   *
   * @return the condition
   */
  public String getCondition() {
    return condition;
  }

  /**
   * Sets the condition on which rule actions will be applied.
   *
   * @param condition the condition to set
   */
  public void setCondition(String condition) {
    this.condition = condition;
  }

  /**
   * Returns pre-process missingLookupPolicy.
   *
   * @return missing lookup policy
   */
  public MissingAttributePolicyV1 getMissingLookupPolicy() {
    return missingLookupPolicy;
  }

  public void setMissingLookupPolicy(MissingAttributePolicyV1 policy) {
    this.missingLookupPolicy = policy;
  }

  /**
   * Returns array of actions to be applied to incoming stream.
   *
   * @return the actions
   */
  public List<ActionDesc> getActions() {
    return actions;
  }

  /**
   * Sets actions to be applied on the incoming stream.
   *
   * @param actions the actions to set
   */
  public void setActions(List<ActionDesc> actions) {
    this.actions = actions;
  }

  /**
   * Adds an action.
   *
   * @param action The action to add.
   * @return self
   */
  public PreprocessDesc addAction(ActionDesc action) {
    if (actions == null) {
      actions = new ArrayList<>();
    }
    actions.add(action);
    return this;
  }
}
