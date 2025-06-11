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
package io.isima.bios.deli.flow;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public abstract class Transformer {
  protected final Function<List<Object>, Object> transformer;
  protected final String as;

  protected Transformer(Function<List<Object>, Object> transformer, String as) {
    Objects.requireNonNull(transformer);
    Objects.requireNonNull(as);
    this.transformer = transformer;
    this.as = as;
  }

  public void transform(List<Object> inputData, Map<String, Object> outRecord) {
    Objects.requireNonNull(inputData);
    Objects.requireNonNull(outRecord);
    outRecord.put(as, transformer.apply(inputData));
  }
}
