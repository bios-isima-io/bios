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
package io.isima.bios.codec.proto.isql;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class BasicBuilderValidatorTest {

  @Test
  public void validateStringList() {
    List<String> arg = new ArrayList<>();
    arg.add("a,b,c");
    arg.add("a,b,c");
    var validator = new BasicBuilderValidator();
    validator.validateStringList(arg, "test");
  }
}
