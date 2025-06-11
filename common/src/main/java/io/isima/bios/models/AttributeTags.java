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
import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@EqualsAndHashCode
@ToString
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AttributeTags {

  private AttributeCategory category;
  private AttributeKind kind;
  private String otherKindName;
  private Unit unit;
  private String unitDisplayName;
  private UnitDisplayPosition unitDisplayPosition;
  private PositiveIndicator positiveIndicator;
  private AttributeSummary firstSummary;
  private AttributeSummary secondSummary;

  /** Merges the two tags objects and return the effective tags, with userTags taking priority. */
  public static AttributeTags getEffectiveTags(
      AttributeType attributeType, AttributeTags userTags, AttributeTags inferredTags) {
    // If there are no tags at all (e.g. for enriched attributes), add a default
    // category and firstSummary.
    if ((userTags == null) && (inferredTags == null)) {
      final var tags = new AttributeTags();
      if ((attributeType == AttributeType.INTEGER) || (attributeType == AttributeType.DECIMAL)) {
        tags.setCategory(AttributeCategory.QUANTITY);
        tags.setFirstSummary(AttributeSummary.AVG);
      } else if ((attributeType == AttributeType.STRING)
          || (attributeType == AttributeType.BOOLEAN)) {
        tags.setCategory(AttributeCategory.DESCRIPTION);
        tags.setFirstSummary(AttributeSummary.WORD_CLOUD);
      }
      return tags;
    }

    // At least one input is not null.
    if (userTags == null) {
      return inferredTags;
    } else if (inferredTags == null) {
      return userTags;
    }

    // Both inputs are non-null; build a new object that merges them.
    final var tags = new AttributeTags();
    final var t1 = userTags;
    final var t2 = inferredTags;
    tags.category = (t1.category != null) ? t1.category : t2.category;
    tags.kind = (t1.kind != null) ? t1.kind : t2.kind;
    tags.otherKindName = (t1.otherKindName != null) ? t1.otherKindName : t2.otherKindName;
    tags.unit = (t1.unit != null) ? t1.unit : t2.unit;
    tags.unitDisplayName = (t1.unitDisplayName != null) ? t1.unitDisplayName : t2.unitDisplayName;
    tags.unitDisplayPosition =
        (t1.unitDisplayPosition != null) ? t1.unitDisplayPosition : t2.unitDisplayPosition;
    tags.positiveIndicator =
        (t1.positiveIndicator != null) ? t1.positiveIndicator : t2.positiveIndicator;
    tags.firstSummary = (t1.firstSummary != null) ? t1.firstSummary : t2.firstSummary;
    tags.secondSummary = (t1.secondSummary != null) ? t1.secondSummary : t2.secondSummary;

    return tags;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof AttributeTags)) {
      return false;
    }
    final var that = (AttributeTags) other;
    return Objects.equals(category, that.category)
        && Objects.equals(kind, that.kind)
        && Objects.equals(otherKindName, that.otherKindName)
        && Objects.equals(unit, that.unit)
        && Objects.equals(unitDisplayName, that.unitDisplayName)
        && Objects.equals(unitDisplayPosition, that.unitDisplayPosition)
        && Objects.equals(positiveIndicator, that.positiveIndicator)
        && Objects.equals(firstSummary, that.firstSummary)
        && Objects.equals(secondSummary, that.secondSummary);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        category,
        kind,
        otherKindName,
        unit,
        unitDisplayName,
        unitDisplayPosition,
        positiveIndicator,
        firstSummary,
        secondSummary);
  }
}
