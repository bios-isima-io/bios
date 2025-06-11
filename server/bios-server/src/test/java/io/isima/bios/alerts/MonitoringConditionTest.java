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
package io.isima.bios.alerts;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import io.isima.bios.vigilantt.exceptions.InvalidRuleException;
import io.isima.bios.vigilantt.grammar.node.ExpressionTreeNode;
import io.isima.bios.vigilantt.grammar.parser.ExpressionParser;
import org.junit.Before;
import org.junit.Test;

public class MonitoringConditionTest {

  private ExpressionParser parser;

  @Before
  public void setUp() {
    parser = new ExpressionParser();
  }

  @Test
  public void testSystemMonitoringConditions() throws Exception {
    String[] conditions =
        new String[] {
          "(max(cpu_usage_percent) > 80)",
          "(max(memory_usage_percent) < 60.2)",
          "(sum(network_in_requests) > 10000)",
          "(sum(network_out_requests) > 10000)"
        };
    // Expected token contents with DFS traverse
    String[][] results =
        new String[][] {
          new String[] {">", "max(cpu_usage_percent)", null, null, "80", null, null},
          new String[] {"<", "max(memory_usage_percent)", null, null, "60.2", null, null},
          new String[] {">", "sum(network_in_requests)", null, null, "10000", null, null},
          new String[] {">", "sum(network_out_requests)", null, null, "10000", null, null},
        };
    runTest(conditions, results);
  }

  @Test
  public void testDiskMonitoringConditions() throws Exception {
    String[] conditions =
        new String[] {"(sum(disk_read_requests) > 1000)", "(sum(disk_write_requests) < 100)"};
    // Expected token contents with DFS traverse
    String[][] results =
        new String[][] {
          new String[] {">", "sum(disk_read_requests)", null, null, "1000", null, null},
          new String[] {"<", "sum(disk_write_requests)", null, null, "100", null, null},
        };
    runTest(conditions, results);
  }

  @Test
  public void testMonitoringCount() throws Exception {
    String[] conditions = new String[] {"(count() > 10000)", "(count() < 100)"};
    // Expected token contents with DFS traverse
    String[][] results =
        new String[][] {
          new String[] {">", "count()", null, null, "10000", null, null},
          new String[] {"<", "count()", null, null, "100", null, null},
        };
    runTest(conditions, results);
  }

  @Test(expected = InvalidRuleException.class)
  public void testInvalidExpressionCausingStackEmptyException() throws Exception {
    String condition = "(cpu_usage_percent)";
    parser.processExpression(condition);
  }

  private void runTest(String[] conditions, String[][] results) throws Exception {
    for (int i = 0; i < conditions.length; ++i) {
      final String condition = conditions[i];
      ExpressionTreeNode node = parser.processExpression(condition);
      String[] expected = results[i];
      assertEquals(expected.length, verify(i, node, expected, 0));
    }
  }

  private int verify(int entryId, ExpressionTreeNode node, String[] expected, int index) {
    String message = "entry=" + entryId + ", index=" + index;
    if (index == expected.length) {
      assertNull(node);
      return index;
    }
    String tokenContent = expected[index];
    if (node == null) {
      assertNull(message, tokenContent);
      return index + 1;
    }
    assertEquals(message, tokenContent, node.getToken().getContent());
    index = verify(entryId, node.getLeft(), expected, index + 1);
    return verify(entryId, node.getRight(), expected, index);
  }
}
