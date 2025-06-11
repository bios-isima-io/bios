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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.RandomUtils;

/**
 * @author sourav
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class StreamConfig {

  /**
   * Stream name.
   */
  String name;

  String signalName;

  public String getSignalName() {
    return signalName;
  }

  public void setSignalName(final String signalName) {
    this.signalName = signalName;
    setName(signalName);
    this.type = StreamType.SIGNAL;
  }

  String contextName;

  public String getContextName() {
    return contextName;
  }

  public void setContextName(final String contextName) {
    this.contextName = contextName;
    setName(contextName);
    this.type = StreamType.CONTEXT;
  }

  Long version;

  /**
   * Expected Peak request size.
   * applicable only for normal distribution.
   */
  int peakRequestSize;

  String httpEndpoint;

  @JsonIgnore
  Boolean deleted;

  StreamType type = StreamType.SIGNAL;

  List<AttributeDesc> attributes;

  List<SignalMap> mappedSignals;

  List<String> keyAttributes;

  /**
   * Default constructor.
   */
  public StreamConfig() {
    attributes = new ArrayList<AttributeDesc>();
  }

  public StreamType getType() {
    return type;
  }

  /**
   * Return name of the stream.
   *
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * Set name of the stream.
   *
   * @param name the name to set
   */
  public StreamConfig setName(String name) {
    this.name = name;
    return this;
  }

  /**
   * Returns event attributes.
   *
   * @return the eventAttributes
   */
  public List<AttributeDesc> getAttributes() {
    return attributes;
  }

  public void setAttributes(List<AttributeDesc> eventAttributes) {
    this.attributes = eventAttributes;
  }

  public Long getVersion() {
    return version;
  }

  public StreamConfig setVersion(Long version) {
    this.version = version;
    return this;
  }

  public boolean isDeleted() {
    return deleted != null ? deleted : false;
  }

  public StreamConfig setDeleted(boolean isDeleted) {
    this.deleted = isDeleted;
    return this;
  }

  public int getPeakRequestSize() {
    if (0 == peakRequestSize) {
      return RandomUtils.nextInt(2000, 5000);
    }
    return peakRequestSize;
  }

  public void setPeakRequestSize(int peakRequestSize) {
    this.peakRequestSize = peakRequestSize;
  }

  public String getHttpEndpoint() {
    return this.httpEndpoint;
  }

  public void setHttpEndpoint(String httpEndpoint) {
    this.httpEndpoint = httpEndpoint;
  }

  public List<SignalMap> getMappedSignal() {
    return mappedSignals;
  }

  public void setMappedSignal(List<SignalMap> mappedSignal) {
    this.mappedSignals = mappedSignal;
  }

  public List<String> getKeyAttributes() {
    return keyAttributes;
  }

  public void setKeyAttributes(List<String> keyAttributes) {
    this.keyAttributes = keyAttributes;
  }

  public boolean isAttrMapped(String attrName) {
    if (mappedSignals != null && !mappedSignals.isEmpty()) {
      for (SignalMap mappedSignal : mappedSignals) {
        if(mappedSignal.getSourceKeyAttributes().equalsIgnoreCase(attrName)) {
          return true;
        }
      }
    }
    return false;
  }

  public SignalMap getMappedSignal(String attrName) {
    if (mappedSignals != null && !mappedSignals.isEmpty()) {
      for (SignalMap mappedSignal : mappedSignals) {
        if (mappedSignal.getSourceKeyAttributes().equalsIgnoreCase(attrName)) {
          return mappedSignal;
        }
      }
    }
    return null;
  }

  public boolean attrMarkedAsKey(String attrToCheck) {
    if(null == keyAttributes) {
      return false;
    }
    for(String key : keyAttributes) {
      if(key.equalsIgnoreCase(attrToCheck)) {
        return true;
      }
    }
    return false;
  }

}
