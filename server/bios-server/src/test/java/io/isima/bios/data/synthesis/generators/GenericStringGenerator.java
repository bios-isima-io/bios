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

public class GenericStringGenerator implements AttributeGenerator<String> {

  private final int length;
  private final Random random;
  private final char[] characters;

  public GenericStringGenerator(int length) {
    if (length <= 0) {
      throw new IllegalArgumentException("length must be a positive number");
    }
    this.length = length;
    random = new Random();
    characters = new char[52];
    for (int i = 0; i < 26; ++i) {
      characters[i] = (char) ('a' + i);
      characters[i + 26] = (char) ('A' + i);
    }
  }

  @Override
  public String generate() {
    char[] content = new char[length];
    for (int i = 0; i < length; ++i) {
      content[i] = characters[random.nextInt(52)];
    }
    return new String(content);
  }
}
