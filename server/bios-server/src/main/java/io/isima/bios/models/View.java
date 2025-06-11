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
import io.isima.bios.models.v1.validators.ValidatorConstants;
import io.isima.bios.req.MutableView;
import java.util.List;
import java.util.Objects;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;

/** Captures the supported view functionaility. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class View implements MutableView {

  @NotNull ViewFunction function;

  @NotNull
  @Size(min = 1, max = 40)
  @Pattern(regexp = ValidatorConstants.NAME_PATTERN)
  String by;

  /** For multiple dimensions. Not supported by SDK yet. */
  List<String> dimensions;

  // sort parameters ///////////////
  Boolean reverse = Boolean.FALSE;
  Boolean caseSensitive = Boolean.FALSE;

  @Deprecated
  public View() {}

  protected View(ViewFunction function) {
    this.function = function;
  }

  @Override
  public ViewFunction getFunction() {
    return function;
  }

  @Override
  @Deprecated
  public void setFunction(ViewFunction function) {
    this.function = function;
  }

  @Override
  public String getBy() {
    return by;
  }

  @Override
  public void setBy(String by) {
    this.by = by;
  }

  @Override
  public List<String> getDimensions() {
    return dimensions;
  }

  public void setDimensions(List<String> dimensions) {
    this.dimensions = dimensions;
  }

  @Override
  public Boolean getReverse() {
    return reverse;
  }

  public void setReverse(Boolean reverse) {
    this.reverse = reverse;
  }

  @Override
  public Boolean getCaseSensitive() {
    return caseSensitive;
  }

  public void setCaseSensitive(Boolean caseSensitive) {
    this.caseSensitive = caseSensitive;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("function='").append(function.name());
    if (by != null) {
      sb.append(", by=").append(by);
    }
    if (dimensions != null && !dimensions.isEmpty()) {
      sb.append(", by=").append(dimensions);
    }
    if (reverse != null) {
      sb.append(", reverse=").append(reverse);
    }
    if (caseSensitive != null) {
      sb.append(", caseSensitive=").append(caseSensitive);
    }
    return sb.toString();
  }

  @Override
  public boolean equals(Object target) {
    if (target == this) {
      return true;
    }
    if (!(target instanceof View)) {
      return false;
    }
    final View that = (View) target;

    if (this.function != null) {
      if (!this.function.equals(that.function)) {
        return false;
      }
    } else if (that.function != null) {
      return false;
    }

    if (this.by != null) {
      if (!this.by.equals(that.by)) {
        return false;
      }
    } else if (that.by != null) {
      return false;
    }

    if (this.dimensions != null) {
      if (!this.dimensions.equals(that.dimensions)) {
        return false;
      }
    } else if (that.dimensions != null) {
      return false;
    }

    if (this.reverse != null) {
      if (!this.reverse.equals(that.reverse)) {
        return false;
      }
    } else if (that.reverse != null) {
      return false;
    }

    if (this.caseSensitive != null) {
      if (!this.caseSensitive.equals(that.caseSensitive)) {
        return false;
      }
    } else if (that.caseSensitive != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hash(function, by, dimensions, reverse, caseSensitive);
  }
}
