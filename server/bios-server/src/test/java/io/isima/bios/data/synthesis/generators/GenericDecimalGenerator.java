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

import java.util.Random;

public class GenericDecimalGenerator implements AttributeGenerator<Double> {

  private final double offset;
  private final double interval;
  private final Random random;

  public GenericDecimalGenerator(double minimum, double maximum) {
    if (minimum >= maximum) {
      throw new IllegalArgumentException("maximum must be larger than minimum");
    }
    offset = minimum;
    interval = maximum - minimum;
    random = new Random();
  }

  @Override
  public Double generate() {
    return Double.valueOf((long) (random.nextDouble() * interval) + offset);
  }
}
