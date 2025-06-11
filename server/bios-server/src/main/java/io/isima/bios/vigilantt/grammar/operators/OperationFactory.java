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
package io.isima.bios.vigilantt.grammar.operators;

import java.util.HashMap;
import java.util.Map;

public class OperationFactory {

  static Map<String, Operation> operationMap = new HashMap<>();

  static {
    operationMap.put("+", new AdditionOperation());
    operationMap.put("-", new SubtractionOperation());
    operationMap.put("*", new MultiplicationOperation());
    operationMap.put("/", new DivisionOperation());
    operationMap.put(">", new GreaterThanOperation());
    operationMap.put("<", new LessThanOperation());
    operationMap.put(">=", new GreaterThanEqualOperation());
    operationMap.put("<=", new LessThanEqualOperation());
    operationMap.put("==", new EqualOperation());
    operationMap.put("contains", new ContainsOperation());
  }

  public static Operation getOperation(String operation) {
    return operationMap.get(operation.toLowerCase());
  }
}
