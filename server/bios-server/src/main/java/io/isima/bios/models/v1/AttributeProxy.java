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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * An immutable class to hold proxy and related information for an attribute. This is meant to be
 * used as a value type in a map stored in the stream.
 */
@EqualsAndHashCode
@Getter
@ToString
public class AttributeProxy {
  /**
   * Short integers used instead of attribute name/version in database tables to save space. These
   * are assigned to each attribute, including additional attributes from enrichments). This number
   * is changed when an attribute's datatype is changed. The proxy number is changed even if the
   * datatype change is made in such a way that data is convertible from old datatype to new
   * datatype. This is because the internal data structures of data sketches depend on the exact
   * datatype of the attribute values, and we need to build new data sketch structures when the
   * datatype changes.
   */
  private final short proxy;

  /**
   * The first version from which an attribute's data is available, including data from before
   * convertible datatype changes. An attribute is assigned the version of the stream when it first
   * gets created. Later, if the datatype is changed to a convertible datatype, the old version is
   * retained for use by a newly generated proxy number. If the datatype is changed to a
   * non-convertible datatype, the version is updated to the stream's new version.
   */
  private final long baseVersion;

  /**
   * The current datatype of the attribute. While this information is a little redundant (i.e. it
   * can be deduced from other places), keeping it here makes it very straightforward for the
   * codebase to access the previous datatype when an attribute's datatype is changed.
   */
  private final InternalAttributeType type;

  @JsonCreator
  public AttributeProxy(
      @JsonProperty("proxy") short proxy,
      @JsonProperty("baseVersion") long baseVersion,
      @JsonProperty("type") InternalAttributeType type) {
    this.proxy = proxy;
    this.baseVersion = baseVersion;
    this.type = type;
  }
}
