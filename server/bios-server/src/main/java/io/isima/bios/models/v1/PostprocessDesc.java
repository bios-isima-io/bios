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
import io.isima.bios.models.Rollup;
import java.util.List;
import java.util.Objects;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.ToString;

/** Postprocess descriptor used for metrics aggregations. */
@JsonIgnoreProperties(ignoreUnknown = true)
@ToString
public class PostprocessDesc extends Duplicatable {

  @NotNull private String view;

  @NotNull @Valid private List<Rollup> rollups;

  public String getView() {
    return view;
  }

  public void setView(String view) {
    this.view = view;
  }

  public List<Rollup> getRollups() {
    return rollups;
  }

  public void setRollups(final List<Rollup> rollups) {
    this.rollups = rollups;
  }

  /**
   * Method to make a clone of the instance.
   *
   * @return duplicated instance.
   */
  @Override
  public PostprocessDesc duplicate() {
    PostprocessDesc clone = new PostprocessDesc();
    clone.view = view;
    clone.rollups = copyList(rollups);
    return clone;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof PostprocessDesc)) {
      return false;
    }

    final PostprocessDesc test = (PostprocessDesc) o;
    if (!(view == null ? test.view == null : view.equalsIgnoreCase(test.view))) {
      return false;
    }
    if (!(rollups == null ? test.rollups == null : rollups.equals(test.rollups))) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hash(view, rollups);
  }
}
