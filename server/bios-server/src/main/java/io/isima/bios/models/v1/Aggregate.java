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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import io.isima.bios.models.MetricFunction;
import io.isima.bios.models.v1.validators.ValidatorConstants;
import io.isima.bios.req.MutableAggregate;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/** A POJO class to represent an aggregate function. */
@EqualsAndHashCode
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class Aggregate implements MutableAggregate {

  @NotNull protected MetricFunction function;

  @Size(min = 1, max = 40)
  @Pattern(regexp = ValidatorConstants.NAME_PATTERN)
  protected String by;

  /**
   * Default output name of an aggregate function is <i>function_name</i>(<i>attribute</i>), such as
   * <code>sum(amount)</code>. This field overrides this default name by specified one.
   */
  @Size(min = 1, max = 40)
  @Pattern(regexp = ValidatorConstants.NAME_PATTERN)
  private String as;

  @Setter(AccessLevel.NONE)
  @EqualsAndHashCode.Exclude
  @ToString.Exclude
  private String outputAttributeName;

  @Deprecated
  public Aggregate() {}

  public Aggregate(MetricFunction function) {
    this.function = function;
  }

  public Aggregate(MetricFunction function, String by) {
    this.function = function;
    this.by = by;
  }

  @Deprecated
  public Aggregate(MetricFunction function, String by, String as) {
    this.function = function;
    this.by = by;
    this.as = as;
  }

  @Deprecated
  public void setFunction(MetricFunction function) {
    this.function = function;
  }

  /**
   * Method to get output attribute name.
   *
   * <p>The method is meant to be called after other properties are filled. The attribute name is
   * calculated at the first time when this method is called. Parameter 'as' needs to be initialized
   * by the time of the calling this method.
   *
   * @return Output attribute name
   */
  @Override
  public String getOutputAttributeName() {
    if (outputAttributeName == null) {
      if (as != null) {
        outputAttributeName = as;
      } else {
        outputAttributeName =
            String.format("%s(%s)", function.name().toLowerCase(), (by != null ? by : ""));
      }
    }
    return outputAttributeName;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("{function=").append(function.name());
    if (by != null) {
      sb.append(", by=").append(by);
    }
    if (as != null) {
      sb.append(", as=").append(as);
    }
    return sb.append("}").toString();
  }
}
