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

/**
 * Interface for Deterministic Finite Automata for individual states during tokenization of input
 * rule string.
 */
public interface TokenizerState {

  /**
   * Consume next character in rule string and return the next transition state.
   *
   * @return Next transition state in DFA
   */
  TokenizerState consumeChar(char ch) throws InvalidRuleException;

  /** Terminate the DFA at this state. */
  void terminate() throws InvalidRuleException;
}
