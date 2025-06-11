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
package io.isima.bios.vigilantt.monitoring;

import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.vigilantt.grammar.evaluator.ExpressionEvaluator;
import io.isima.bios.vigilantt.grammar.node.ExpressionTreeNode;
import io.isima.bios.vigilantt.grammar.parser.ExpressionParser;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** TODO - Move this class to monitoring module in server */
public class MonitoringRulesTest {
  private ExpressionParser expressionParser;

  @Before
  public void setUp() {
    expressionParser = new ExpressionParser();
  }

  @After
  public void tearDown() {}

  @Test
  public void testValidRulesAreParsedSuccessfully() throws Exception {
    String[] validMonitoringRules = new String[] {"(cpu_usage > 80)", "(disk_available < 30)"};
    for (String monitoringRule : validMonitoringRules) {
      ExpressionTreeNode root = expressionParser.processExpression(monitoringRule);
      Assert.assertNotNull(root);
    }
  }

  @Test
  public void testValidComplexRulesAreParsedSuccessfully() throws Exception {
    String[] complexMonitoringRules =
        new String[] {
          "((latency > 0.1)  AND (disk_usage > 80))",
          "(((latency > 0.1)  AND (disk_usage > 80)) OR (memory_usage > 90))",
          "(((latency > 0.1)  AND (disk_usage > 80)) OR ((memory_usage > 90) "
              + "AND (num_process > 1000)))"
        };
    for (String monitoringRule : complexMonitoringRules) {
      ExpressionTreeNode root = expressionParser.processExpression(monitoringRule);
      Assert.assertNotNull(root);
    }
  }

  @Test
  public void testRuleEvaluatesToTrueForAnomalousEvents() throws Exception {
    String monitoringRule = "(cpu_usage > 80)";
    ExpressionTreeNode root = expressionParser.processExpression(monitoringRule);
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("cpu_usage", 95);
    attributes.put("disk_available", 25);
    Event event = new EventJson();
    event.setAttributes(attributes);

    ExpressionEvaluator.evaluateExpression(root, event);
    Boolean result = (Boolean) root.getResult();
    Assert.assertTrue(result);
  }

  @Test
  public void testRuleEvaluatesToFalseForNormalEvents() throws Exception {
    String monitoringRule = "(cpu_usage > 80)";
    ExpressionTreeNode root = expressionParser.processExpression(monitoringRule);
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("cpu_usage", 50);
    attributes.put("disk_available", 25);
    Event event = new EventJson();
    event.setAttributes(attributes);

    ExpressionEvaluator.evaluateExpression(root, event);
    Boolean result = (Boolean) root.getResult();
    Assert.assertFalse(result);
  }

  @Test
  public void testEqualityRuleWithStringAttribute() throws Exception {
    String monitoringRule = "(exception == 'OutOfMemoryException')";
    ExpressionTreeNode root = expressionParser.processExpression(monitoringRule);

    Map<String, Object> attributes = new HashMap<>();
    attributes.put("exception", "OutOfMemoryException");
    attributes.put("disk_available", 25);
    Event event = new EventJson();
    event.setAttributes(attributes);

    ExpressionEvaluator.evaluateExpression(root, event);
    Boolean result = (Boolean) root.getResult();
    Assert.assertTrue(result);
  }

  @Test
  public void testRulesWithStringAttributesPositiveCases() throws Exception {
    String monitoringRule = "(exception contains 'OutOfMemoryException')";
    ExpressionTreeNode root = expressionParser.processExpression(monitoringRule);
    String[] eventsWhichSatisfyRule =
        new String[] {
          "Got OutOfMemoryException while fetching events",
          "OutOfMemoryException while running algorithm",
          "Exception in thread main java.lang.OutOfMemoryException: Java heap space",
          "java.lang.OutOfMemoryException: GC overhead limit exceeded",
        };

    for (String eventString : eventsWhichSatisfyRule) {
      Map<String, Object> attributes = new HashMap<>();
      attributes.put("exception", eventString);
      attributes.put("disk_available", 25);
      Event event = new EventJson();
      event.setAttributes(attributes);

      ExpressionEvaluator.evaluateExpression(root, event);
      Boolean result = (Boolean) root.getResult();
      Assert.assertTrue(result);
    }
  }

  @Test
  public void testRulesWithStringAttributesNegativeCases() throws Exception {
    String monitoringRule = "(exception contains 'OutOfMemoryException')";
    ExpressionTreeNode root = expressionParser.processExpression(monitoringRule);
    String[] eventsWhichDontSatisfyRule =
        new String[] {
          "15:12:13,953 INFO  [org.jboss.as.server.deployment.scanner]",
          "15:12:34,953 INFO  Ingest event started",
          "15:12:58,312 INFO  [com.datastax.driver.core.ClockFactory]",
          "15:13:23,312 WARN  [Cannot connect to Cassandra]"
        };

    for (String eventString : eventsWhichDontSatisfyRule) {
      Map<String, Object> attributes = new HashMap<>();
      attributes.put("exception", eventString);
      attributes.put("disk_available", 25);
      Event event = new EventJson();
      event.setAttributes(attributes);

      ExpressionEvaluator.evaluateExpression(root, event);
      Boolean result = (Boolean) root.getResult();
      Assert.assertFalse(result);
    }
  }
}
