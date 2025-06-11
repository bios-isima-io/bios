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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class SignalMap {

  @JsonProperty(value="destSignalName")
  private String destSignalName;

  @JsonProperty(value="srcAttribute")
  private String  sourceKeyAttribute;

  @JsonProperty(value="destAttribute")
  private String  destinationKeyAttribute;


  public String getDestSignalName() {
    return destSignalName;
  }

  public void setDestSignalName(String destSignalName) {
    this.destSignalName = destSignalName;
  }

  public String getSourceKeyAttributes() {
    return sourceKeyAttribute;
  }

  public void setSourceKeyAttributes(String sourceKeyAttributes) {
    this.sourceKeyAttribute = sourceKeyAttributes;
  }

  public String getDestinationKeyAttributes() {
    return destinationKeyAttribute;
  }

  public void setDestinationKeyAttributes(String destinationKeyAttributes) {
    this.destinationKeyAttribute = destinationKeyAttributes;
  }

}
