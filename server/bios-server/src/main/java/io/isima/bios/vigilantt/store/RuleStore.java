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
package io.isima.bios.vigilantt.store;

import io.isima.bios.vigilantt.exceptions.InvalidRuleException;
import io.isima.bios.vigilantt.exceptions.NoSuchRuleException;
import io.isima.bios.vigilantt.models.Rule;
import java.util.List;

/**
 * Interface to manage rules for monitoring. This is an abstraction for CRUD on rules Possible
 * Implementations are InMemoryRuleStore, CassandraBackedRuleStore
 */
public interface RuleStore {

  /**
   * Create a rule with given rule definition
   *
   * @param rule input rule POJO
   */
  String createRule(Rule rule) throws InvalidRuleException;

  /**
   * Fetch a rule given its rule id
   *
   * @param ruleId unique rule id
   * @return created rule POJO
   */
  Rule getRule(String ruleId) throws NoSuchRuleException;

  /**
   * Update rule given rule id and updated rule definition
   *
   * @param ruleId unique rule id
   * @param rule updated rule POJO
   * @throws InvalidRuleException Invalid rule exception
   */
  void updateRule(String ruleId, Rule rule) throws InvalidRuleException;

  /**
   * Delete rule given its rule id
   *
   * @param ruleId unique rule id
   */
  void deleteRule(String ruleId) throws NoSuchRuleException;

  /** Get all registered rules */
  List<Rule> getAllRules();
}
