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
package io.isima.bios.data.synthesis.generators;

import io.isima.bios.models.AttributeValueGeneric;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;

public class GenericStringEnumGenerator implements AttributeGenerator<String> {
  private final List<String> allowedValues;
  private final Random random;

  public GenericStringEnumGenerator(List<AttributeValueGeneric> allowedValues) {
    Objects.requireNonNull(allowedValues);
    this.allowedValues =
        allowedValues.stream().map((value) -> value.asString()).collect(Collectors.toList());
    random = new Random();
  }

  @Override
  public String generate() {
    return allowedValues.get(random.nextInt(allowedValues.size()));
  }

  // only for test
  protected List<String> getAllowedValues() {
    return allowedValues;
  }
}
