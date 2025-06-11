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
package io.isima.bios.dto;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

/** Class that describes a orderBy specification in a select request. */
@Getter
@EqualsAndHashCode
@ToString
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"by", "reverse", "caseSensitive"})
public class SelectOrder {
  @JsonCreator
  public SelectOrder(
      @JsonProperty("by") String keyName,
      @JsonProperty("reverse") boolean reverse,
      @JsonProperty("caseSensitive") boolean caseSensitive) {
    this.keyName = requireNonNull(keyName, "keyName is null");
    this.reverse = reverse;
    this.caseSensitive = caseSensitive;
  }

  /** Sort key. */
  @NonNull
  @JsonProperty("by")
  private final String keyName;

  /** Sort order. */
  @JsonProperty("reverse")
  private final boolean reverse;

  /** Flag for case sensitivity in case the key is string type. */
  @JsonProperty("caseSensitive")
  private final boolean caseSensitive;
}
