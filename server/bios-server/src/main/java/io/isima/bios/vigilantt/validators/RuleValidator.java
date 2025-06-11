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
package io.isima.bios.vigilantt.validators;

/**
 * This interface consumes a rule string and determines whether it is a valid rule RuleStore uses
 * this abstraction to determine whether input rule is valid Implementations might have a specific
 * grammar which is used to validate the rule
 *
 * <p>For example, below are some possible implementations IntraSignalRuleValidator - validate the
 * rules for expressions with attributes within a signal InterSignalRuleValidator - validate the
 * rules for expressions with attributes across signals MonitoringRuleValidator - validate the rules
 * for expressions with monitoring conditions
 */
public interface RuleValidator {

  /**
   * Return true if the input string is a valid rule
   *
   * @param rule
   * @return boolean
   */
  boolean isValid(String rule);
}
