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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/** Signal schema. */
@Getter
@Setter
@ToString(callSuper = true)
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({
  "signalName",
  "version",
  "biosVersion",
  "missingAttributePolicy",
  "attributes",
  "enrich",
  "postStorageStage",
  "exportDestinationId",
  "isInternal"
})
public class SignalConfig extends BiosStreamConfig {

  /** Enrich configuration. */
  @JsonProperty("enrich")
  private EnrichConfig enrich;

  /** Post storage stage configuration. */
  @JsonProperty("postStorageStage")
  private PostStorageStageConfig postStorageStage;

  public SignalConfig() {}

  public SignalConfig(String name) {
    super.setName(name);
  }

  public SignalConfig(String name, Long version) {
    super.setName(name);
    super.setVersion(version);
  }

  public SignalConfig(SignalConfig base) {
    this(base, true);
  }

  public SignalConfig(SignalConfig base, boolean includeInferredTags) {
    super(base, includeInferredTags);
    // TODO(Naoki): Better to do deep copy?
    this.enrich = base.enrich;
    if (base.postStorageStage != null) {
      this.postStorageStage = new PostStorageStageConfig(base.postStorageStage);
    }
  }

  @Override
  @JsonProperty("signalName")
  public String getName() {
    return super.getName();
  }

  @Override
  @JsonProperty("signalName")
  public void setName(String name) {
    super.setName(name);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof SignalConfig)) {
      return false;
    }
    final var that = (SignalConfig) other;
    return super.equals(other)
        && Objects.equals(enrich, that.enrich)
        && Objects.equals(postStorageStage, that.postStorageStage);
  }

  @Override
  public int hashCode() {
    return super.hashCode() + Objects.hash(enrich, postStorageStage);
  }
}
