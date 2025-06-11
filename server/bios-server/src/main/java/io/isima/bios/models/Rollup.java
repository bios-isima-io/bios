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
import javax.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class Rollup extends Duplicatable {

  @NotNull private String name;

  @NotNull private TimeInterval interval;

  @NotNull private TimeInterval horizon;

  protected List<AlertConfig> alerts;

  protected String schemaName;

  /**
   * Table version for the stream. The table version may be different from stream version In case,
   * e.g., only view and post-process has modified.
   */
  protected Long schemaVersion;

  @Override
  public Rollup duplicate() {
    return new Rollup().duplicate(this);
  }

  protected Rollup duplicate(Rollup src) {
    name = src.name;
    horizon = src.horizon;
    interval = src.interval;
    schemaName = src.schemaName;
    schemaVersion = src.schemaVersion;
    alerts = copyList(src.alerts);
    return this;
  }
}
