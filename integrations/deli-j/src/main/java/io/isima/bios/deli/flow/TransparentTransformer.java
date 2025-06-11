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

public class TransparentTransformer extends Transformer {

  protected TransparentTransformer(String as) {
    super(TransparentTransformer::passThrough, as);
  }

  private static Object passThrough(List<Object> inputData) {
    assert inputData != null;
    return inputData.isEmpty() ? null : inputData.get(0);
  }
}
