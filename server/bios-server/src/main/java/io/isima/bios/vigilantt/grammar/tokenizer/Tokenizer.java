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
package io.isima.bios.vigilantt.grammar.tokenizer;

import io.isima.bios.vigilantt.exceptions.InvalidRuleException;
import java.util.ArrayList;
import java.util.List;

public class Tokenizer {

  public static List<Token> tokenize(String rule) throws InvalidRuleException {
    List<Token> tokens = new ArrayList<>();
    TokenizerState tokenizerState = new StartState(tokens);
    for (char token : rule.toCharArray()) {
      tokenizerState = tokenizerState.consumeChar(token);
    }
    tokenizerState.terminate();
    return tokens;
  }
}
