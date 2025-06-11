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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonValue;
import io.isima.bios.utils.EnumStringifier;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true) // not ideal but to avoid surprises on SDK side
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
@EqualsAndHashCode
public class ImportDataProcessorConfig {
  @NotNull private String processorName;

  @NotNull private Encoding encoding;

  @NotNull private String code;

  public enum Encoding {
    BASE64;

    private static final EnumStringifier<Encoding> stringifier = new EnumStringifier<>(values());

    @JsonCreator
    static Encoding forValue(String value) {
      return stringifier.destringify(value);
    }

    @JsonValue
    public String stringify() {
      return stringifier.stringify(this);
    }
  }
}
