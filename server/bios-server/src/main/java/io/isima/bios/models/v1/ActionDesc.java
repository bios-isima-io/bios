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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import io.isima.bios.models.Duplicatable;
import io.isima.bios.models.MissingAttributePolicyV1;
import javax.validation.constraints.NotNull;
import lombok.ToString;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
@ToString
public class ActionDesc extends Duplicatable {

  @NotNull private ActionType actionType;

  private String attribute;

  private String context;

  private MissingAttributePolicyV1 missingLookupPolicy;

  private Object defaultValue;

  @JsonIgnore private Object internalDefaultValue;

  private String as;

  @Override
  public ActionDesc duplicate() {
    ActionDesc clone = new ActionDesc();
    clone.actionType = actionType;
    clone.attribute = attribute;
    clone.context = context;
    clone.missingLookupPolicy = missingLookupPolicy;
    clone.defaultValue = defaultValue;
    clone.internalDefaultValue = internalDefaultValue;
    clone.as = as;
    return clone;
  }

  public ActionType getActionType() {
    return actionType;
  }

  public void setActionType(ActionType actionType) {
    this.actionType = actionType;
  }

  public String getAttribute() {
    return attribute;
  }

  public void setAttribute(String attribute) {
    this.attribute = attribute;
  }

  public String getContext() {
    return context;
  }

  public void setContext(String stream) {
    this.context = stream;
  }

  public MissingAttributePolicyV1 getMissingLookupPolicy() {
    return missingLookupPolicy;
  }

  public void setMissingLookupPolicy(MissingAttributePolicyV1 missingLookupPolicy) {
    this.missingLookupPolicy = missingLookupPolicy;
  }

  public Object getDefaultValue() {
    return defaultValue;
  }

  public void setDefaultValue(Object defaultValue) {
    this.defaultValue = defaultValue;
  }

  public Object getInternalDefaultValue() {
    return internalDefaultValue;
  }

  public void setInternalDefaultValue(Object internalDefaultValue) {
    this.internalDefaultValue = internalDefaultValue;
  }

  public String getAs() {
    return as;
  }

  public void setAs(String as) {
    this.as = as;
  }
}
