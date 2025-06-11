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
import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true) // not ideal but to avoid surprises on SDK side
@Getter
@Setter
@NoArgsConstructor
@ToString
@EqualsAndHashCode
public class DataPickupSpec {
  /** Record filtering specifications. */
  @Valid private List<FilterSpec> filters;

  /**
   * Specifies a condition to delete a context entry. This configuration can be applied only to
   * contexts. A DataPickupSpecs with this property for a signal would be rejected.
   */
  @Valid private DeletionSpec deletionSpec;

  /**
   * Attribute search path. If the path is blank, the importer would find attributes from root (flat
   * key-value pairs).
   */
  @NotNull private String attributeSearchPath;

  @Valid private Attribute tenantForwarding;

  /**
   * Attribute import specifications. An entry of this property is also called "import processing
   * unit".
   */
  @NotNull private List<Attribute> attributes = List.of();

  /** Defines a stage of import source filter. */
  @Getter
  @Setter
  @JsonInclude(Include.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @ToString
  @EqualsAndHashCode
  public static class FilterSpec {
    /** Source attribute name of the filter target. */
    @NotNull private String sourceAttributeName;

    /** Filter method or lambda. */
    @NotNull private String filter;
  }

  /** Defines a stage of import source filter. */
  @Getter
  @Setter
  @JsonInclude(Include.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @ToString
  @EqualsAndHashCode
  public static class DeletionSpec {
    /** Source attribute name of the filter target. */
    @NotNull private String sourceAttributeName;

    /**
     * Lambda to define condition to delete.
     *
     * <p>If the condition returns true, the corresponding entry is deleted
     */
    @NotNull private String condition;
  }

  /** Defines a unit of data pickup specification. */
  @Getter
  @Setter
  @JsonInclude(Include.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @ToString
  @EqualsAndHashCode
  public static class Attribute {
    /** Source attribute name. */
    private String sourceAttributeName;

    /** Source attribute names. */
    private List<String> sourceAttributeNames;

    @Valid private List<ProcessSpec> processes;

    @Valid private List<TransformSpec> transforms;

    /** Optional: Destination attribute name. */
    private String as;
  }

  /** Defines a stage of import source pre-processing. */
  @Getter
  @Setter
  @JsonInclude(Include.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @ToString
  @EqualsAndHashCode
  public static class ProcessSpec {
    /** Name of a dynamic loaded module. */
    @NotNull private String processorName;

    /** Method name to use. */
    @NotNull private String method;
  }

  /** Defines a source to destination attribute transformation. */
  @Getter
  @Setter
  @JsonInclude(Include.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @ToString
  @EqualsAndHashCode
  public static class TransformSpec {
    /** Required: Transformation rule; method or lambda. */
    @NotNull private String rule;

    /** Required: Destination attribute name. */
    @NotNull private String as;
  }
}
