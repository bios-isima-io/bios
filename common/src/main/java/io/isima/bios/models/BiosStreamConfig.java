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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class BiosStreamConfig {
  /** Signal/Context name. */
  @NotNull private String name;

  /** An optional description that can be set by the user. */
  @JsonProperty("description")
  protected String description;

  /** Context version. */
  @JsonProperty("version")
  private Long version;

  /** Context storage schema version. */
  @JsonProperty("biosVersion")
  private Long biosVersion;

  /** Behavior of adding entry when an attribute value is missing. */
  @JsonProperty("missingAttributePolicy")
  private MissingAttributePolicy missingAttributePolicy;

  /** Context attributes. */
  @JsonProperty("attributes")
  private List<AttributeConfig> attributes;

  /** External storage for data. */
  @JsonProperty("exportDestinationId")
  private String exportDestinationId;

  @JsonProperty("isInternal")
  private Boolean isInternal;

  public BiosStreamConfig() {
    super();
  }

  public BiosStreamConfig(BiosStreamConfig base, boolean includeInferredTags) {
    this.name = base.name;
    this.description = base.description;
    this.version = base.version;
    this.biosVersion = base.biosVersion;
    this.missingAttributePolicy = base.missingAttributePolicy;
    if (includeInferredTags) {
      this.attributes = base.attributes;
    } else {
      this.attributes =
          base.attributes.stream()
              .map(
                  (attribute) -> {
                    final var newAttr = new AttributeConfig(attribute);
                    newAttr.setInferredTags(null);
                    return newAttr;
                  })
              .collect(Collectors.toList());
    }
    this.exportDestinationId = base.exportDestinationId;
  }

  public void removeInferredInformation() {
    for (final var attribute : attributes) {
      attribute.setInferredTags(null);
    }
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof BiosStreamConfig)) {
      return false;
    }
    final var that = (BiosStreamConfig) other;
    return Objects.equals(name, that.name)
        && Objects.equals(description, that.description)
        && Objects.equals(version, that.version)
        && Objects.equals(biosVersion, that.biosVersion)
        && Objects.equals(missingAttributePolicy, that.missingAttributePolicy)
        && Objects.equals(attributes, that.attributes)
        && Objects.equals(exportDestinationId, that.exportDestinationId)
        && Objects.equals(isInternal, that.isInternal);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        name,
        description,
        version,
        biosVersion,
        missingAttributePolicy,
        attributes,
        exportDestinationId,
        isInternal);
  }
}
