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
package io.isima.bios.admin.v1;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.isima.bios.admin.v1.impl.SharedPropertiesImpl;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.models.LbPolicy;
import io.isima.bios.models.NodeType;
import io.isima.bios.models.OperationPolicy;
import io.isima.bios.models.OperationSet;
import io.isima.bios.models.OperationType;
import io.isima.bios.models.Upstream;
import io.isima.bios.models.UpstreamConfig;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SharedConfigTest {

  private static SharedConfig sharedConfig;

  private String configBackup;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Bios2TestModules.startModulesWithoutMaintenance(SharedConfigTest.class);
    sharedConfig = BiosModules.getSharedConfig();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Bios2TestModules.shutdown();
  }

  @Before
  public void setUp() throws Exception {
    SharedPropertiesImpl properties = (SharedPropertiesImpl) BiosModules.getSharedProperties();
    configBackup = properties.getProperty(SharedConfig.NODES);
    properties.deleteProperty(SharedConfig.NODES);
  }

  @After
  public void tearDown() throws Exception {
    BiosModules.getSharedProperties().setProperty(SharedConfig.NODES, configBackup);
  }

  @Test
  public void test() throws ApplicationException, ExecutionException, InterruptedException {
    List<String> endpoints = sharedConfig.getEndpoints();

    assertNotNull(endpoints);
    assertTrue(endpoints.isEmpty());

    sharedConfig.addEndpoint("http://172.18.0.11", "bios-signal", NodeType.SIGNAL);
    sharedConfig.addEndpoint("http://172.18.0.12", "bios-rollup", NodeType.ROLLUP);
    sharedConfig.addEndpoint("http://172.18.0.13", "bios-analysis", NodeType.ANALYSIS);
    endpoints = sharedConfig.getEndpoints();

    assertEquals(3, endpoints.size());
    assertTrue(endpoints.contains("http://172.18.0.11"));
    assertTrue(endpoints.contains("http://172.18.0.12"));
    assertTrue(endpoints.contains("http://172.18.0.13"));

    assertEquals("bios-signal", sharedConfig.getNodeName("http://172.18.0.11"));
    assertEquals("bios-rollup", sharedConfig.getNodeName("http://172.18.0.12"));
    assertEquals("bios-analysis", sharedConfig.getNodeName("http://172.18.0.13"));

    assertEquals(
        Pair.of(NodeType.SIGNAL, "http://172.18.0.11"),
        sharedConfig.nodeNameToEndpoint("bios-signal"));
    assertEquals(
        Pair.of(NodeType.ROLLUP, "http://172.18.0.12"),
        sharedConfig.nodeNameToEndpoint("bios-rollup"));
    assertEquals(
        Pair.of(NodeType.ANALYSIS, "http://172.18.0.13"),
        sharedConfig.nodeNameToEndpoint("bios-analysis"));

    final var state = new GenericExecutionState("test", Executors.newSingleThreadExecutor());
    assertEquals(
        Pair.of(NodeType.SIGNAL, "http://172.18.0.11"),
        sharedConfig.nodeNameToEndpointAsync("bios-signal", state).toCompletableFuture().get());
    assertEquals(
        Pair.of(NodeType.ROLLUP, "http://172.18.0.12"),
        sharedConfig.nodeNameToEndpointAsync("bios-rollup", state).toCompletableFuture().get());
    assertEquals(
        Pair.of(NodeType.ANALYSIS, "http://172.18.0.13"),
        sharedConfig.nodeNameToEndpointAsync("bios-analysis", state).toCompletableFuture().get());

    sharedConfig.removeEndpoint("http://172.18.0.12", NodeType.ROLLUP);
    endpoints = sharedConfig.getEndpoints();

    assertEquals(2, endpoints.size());
    assertTrue(endpoints.contains("http://172.18.0.11"));
    assertTrue(endpoints.contains("http://172.18.0.13"));

    assertEquals("bios-signal", sharedConfig.getNodeName("http://172.18.0.11"));
    assertNull(sharedConfig.getNodeName("http://172.18.0.12"));
    assertEquals("bios-analysis", sharedConfig.getNodeName("http://172.18.0.13"));

    assertEquals(
        Pair.of(NodeType.SIGNAL, "http://172.18.0.11"),
        sharedConfig.nodeNameToEndpoint("bios-signal"));
    assertNull(sharedConfig.nodeNameToEndpoint("bios-rollup"));
    assertEquals(
        Pair.of(NodeType.ANALYSIS, "http://172.18.0.13"),
        sharedConfig.nodeNameToEndpoint("bios-analysis"));

    assertEquals(
        Pair.of(NodeType.SIGNAL, "http://172.18.0.11"),
        sharedConfig.nodeNameToEndpointAsync("bios-signal", state).toCompletableFuture().get());
    assertNull(
        sharedConfig.nodeNameToEndpointAsync("bios-rollup", state).toCompletableFuture().get());
    assertEquals(
        Pair.of(NodeType.ANALYSIS, "http://172.18.0.13"),
        sharedConfig.nodeNameToEndpointAsync("bios-analysis", state).toCompletableFuture().get());

    sharedConfig.removeEndpoint("http://172.18.0.11", NodeType.SIGNAL);
    sharedConfig.removeEndpoint("http://172.18.0.13", NodeType.ANALYSIS);

    endpoints = sharedConfig.getEndpoints();
    assertEquals(0, endpoints.size());

    assertNull(sharedConfig.getNodeName("http://172.18.0.11"));
    assertNull(sharedConfig.getNodeName("http://172.18.0.12"));
    assertNull(sharedConfig.getNodeName("http://172.18.0.13"));

    assertNull(sharedConfig.nodeNameToEndpoint("bios-signal"));
    assertNull(sharedConfig.nodeNameToEndpoint("bios-rollup"));
    assertNull(sharedConfig.nodeNameToEndpoint("bios-analysis"));

    assertNull(
        sharedConfig.nodeNameToEndpointAsync("bios-signal", state).toCompletableFuture().get());
    assertNull(
        sharedConfig.nodeNameToEndpointAsync("bios-rollup", state).toCompletableFuture().get());
    assertNull(
        sharedConfig.nodeNameToEndpointAsync("bios-analysis", state).toCompletableFuture().get());
  }

  @Test
  public void testContextEndpoints() throws ApplicationException {
    List<String> contextEndpoints = sharedConfig.getContextEndpoints();

    assertNotNull(contextEndpoints);
    assertTrue(contextEndpoints.isEmpty());

    sharedConfig.addEndpoint("http://172.18.0.11", "bios-signal", NodeType.SIGNAL);
    sharedConfig.addEndpoint("http://172.18.0.12", "bios-rollup", NodeType.ROLLUP);
    sharedConfig.addEndpoint("http://172.18.0.13", "bios-analysis", NodeType.ANALYSIS);
    contextEndpoints = sharedConfig.getContextEndpoints();

    assertEquals(1, contextEndpoints.size());
    assertTrue(contextEndpoints.contains("http://172.18.0.11"));

    sharedConfig.removeEndpoint("http://172.18.0.12", NodeType.ROLLUP);
    contextEndpoints = sharedConfig.getContextEndpoints();

    assertEquals(1, contextEndpoints.size());
    assertTrue(contextEndpoints.contains("http://172.18.0.11"));

    sharedConfig.removeEndpoint("http://172.18.0.11", NodeType.SIGNAL);
    contextEndpoints = sharedConfig.getContextEndpoints();

    assertEquals(0, contextEndpoints.size());
  }

  @Test
  public void testRegisterNode() throws ApplicationException {
    List<String> nodeEndpoints = sharedConfig.getEndpoints();

    assertNotNull(nodeEndpoints);
    assertTrue(nodeEndpoints.isEmpty());

    sharedConfig.registerNode("http://172.18.0.11", "signal", NodeType.SIGNAL);
    nodeEndpoints = sharedConfig.getEndpoints();

    assertEquals(1, nodeEndpoints.size());
    assertTrue(nodeEndpoints.contains("http://172.18.0.11"));

    // register same node again with same node type
    sharedConfig.registerNode("http://172.18.0.11", "signal", NodeType.SIGNAL);
    nodeEndpoints = sharedConfig.getEndpoints();

    assertEquals(1, nodeEndpoints.size());
    assertTrue(nodeEndpoints.contains("http://172.18.0.11"));

    // register same node again with different node type
    sharedConfig.registerNode("http://172.18.0.11", "rollup", NodeType.ROLLUP);
    nodeEndpoints = sharedConfig.getEndpoints();

    assertEquals(1, nodeEndpoints.size());
    assertTrue(nodeEndpoints.contains("http://172.18.0.11"));

    // verify the node type does not get changed
    nodeEndpoints = sharedConfig.getContextEndpoints();

    assertEquals(1, nodeEndpoints.size());
    assertTrue(nodeEndpoints.contains("http://172.18.0.11"));
  }

  @Test
  public void nodeFailureMarkTest() throws Exception {
    // check initial state
    List<String> endpoints = sharedConfig.getEndpoints();

    assertNotNull(endpoints);
    assertTrue(endpoints.isEmpty());

    // add three endpoints
    sharedConfig.addEndpoint("http://172.18.0.11", "bios-signal", NodeType.SIGNAL);
    sharedConfig.addEndpoint("http://172.18.0.12", "bios-rollup", NodeType.ROLLUP);
    sharedConfig.addEndpoint("http://172.18.0.13", "bios-analysis", NodeType.ANALYSIS);

    // check the added endpoints
    endpoints = sharedConfig.getEndpoints();
    assertEquals(3, endpoints.size());

    assertTrue(endpoints.contains("http://172.18.0.11"));
    assertTrue(endpoints.contains("http://172.18.0.12"));
    assertTrue(endpoints.contains("http://172.18.0.13"));

    // mark the second endpoint as failed
    sharedConfig.addFailedEndpoint("http://172.18.0.12");

    // verify the second node is marked as failed
    endpoints = sharedConfig.getEndpoints();

    assertEquals(3, endpoints.size());
    assertTrue(endpoints.contains("http://172.18.0.11"));
    assertTrue(endpoints.contains(".http://172.18.0.12"));
    assertTrue(endpoints.contains("http://172.18.0.13"));

    // remove the failure mark
    sharedConfig.removeFailedEndpoint("http://172.18.0.12");

    // verify the endpoints are back
    endpoints = sharedConfig.getEndpoints();

    assertEquals(3, endpoints.size());
    assertTrue(endpoints.contains("http://172.18.0.11"));
    assertTrue(endpoints.contains("http://172.18.0.12"));
    assertTrue(endpoints.contains("http://172.18.0.13"));
  }

  @Test
  public void testMigrateEndpointToNode_NodesAlreadyExist() throws ApplicationException {
    SharedPropertiesImpl properties = (SharedPropertiesImpl) BiosModules.getSharedProperties();

    // nodes property exist, so nothing to migrate
    StringJoiner nodeJoiner = new StringJoiner(SharedConfig.NODES_DELIMITER);
    String node1 =
        NodeType.SIGNAL.getTypeName() + SharedConfig.NODE_INFO_SEPARATOR + "http://172.18.0.11";
    String node2 =
        NodeType.ROLLUP.getTypeName() + SharedConfig.NODE_INFO_SEPARATOR + "http://172.18.0.12";

    String nodes = nodeJoiner.add(node1).add(node2).toString();
    properties.setProperty(SharedConfig.NODES, nodes);

    sharedConfig.migrateEndpointToNode();

    assertEquals(nodes, properties.getProperty(SharedConfig.NODES));

    List<String> allNodes = sharedConfig.getEndpoints();
    assertEquals(2, allNodes.size());
    assertTrue(allNodes.contains("http://172.18.0.11"));
    assertTrue(allNodes.contains("http://172.18.0.12"));

    List<String> contextNodes = sharedConfig.getContextEndpoints();
    assertEquals(1, contextNodes.size());
    assertTrue(contextNodes.contains("http://172.18.0.11"));

    properties.deleteProperty(SharedConfig.NODES);
  }

  @Test
  public void testMigrateEndpointToNode_EndpointDoesNotExist() throws ApplicationException {
    SharedPropertiesImpl properties = (SharedPropertiesImpl) BiosModules.getSharedProperties();

    // backup endpoints property and remove it
    String endpointsBackup = properties.getProperty(SharedConfig.ENDPOINTS);
    properties.deleteProperty(SharedConfig.ENDPOINTS);

    // endpoints property does not exist, so nothing to migrate
    sharedConfig.migrateEndpointToNode();

    properties.setProperty(SharedConfig.ENDPOINTS, endpointsBackup);

    assertNull(properties.getProperty(SharedConfig.NODES));

    List<String> allNodes = sharedConfig.getEndpoints();
    assertNotNull(allNodes);
    assertTrue(allNodes.isEmpty());

    List<String> contextNodes = sharedConfig.getContextEndpoints();
    assertNotNull(contextNodes);
    assertTrue(contextNodes.isEmpty());
  }

  @Test
  public void testMigrateEndpointToNode() throws ApplicationException {
    SharedPropertiesImpl properties = (SharedPropertiesImpl) BiosModules.getSharedProperties();

    // get endpoints property
    String endpointsBackup = properties.getProperty(SharedConfig.ENDPOINTS);
    properties.deleteProperty(SharedConfig.ENDPOINTS);

    StringJoiner nodeJoiner = new StringJoiner(SharedConfig.NODES_DELIMITER);
    String node1 =
        NodeType.SIGNAL.getTypeName() + SharedConfig.NODE_INFO_SEPARATOR + "http://172.18.0.13";
    String node2 =
        NodeType.SIGNAL.getTypeName() + SharedConfig.NODE_INFO_SEPARATOR + "http://172.18.0.14";
    String expectedNodes = nodeJoiner.add(node1).add(node2).toString();

    String endpoints =
        "http://172.18.0.13" + SharedConfig.ENDPOINTS_DELIMITER + "http://172.18.0.14";

    properties.setProperty(SharedConfig.ENDPOINTS, endpoints);

    // endpoints property exist but nodes does not, migration should happens
    sharedConfig.migrateEndpointToNode();

    assertEquals(expectedNodes, properties.getProperty(SharedConfig.NODES));

    List<String> allNodes = sharedConfig.getEndpoints();
    assertEquals(2, allNodes.size());
    assertTrue(allNodes.contains("http://172.18.0.13"));
    assertTrue(allNodes.contains("http://172.18.0.14"));

    List<String> contextNodes = sharedConfig.getContextEndpoints();
    assertEquals(2, contextNodes.size());
    assertTrue(contextNodes.contains("http://172.18.0.13"));
    assertTrue(contextNodes.contains("http://172.18.0.14"));

    properties.deleteProperty(SharedConfig.NODES);

    properties.deleteProperty(SharedConfig.ENDPOINTS);
    properties.setProperty(SharedConfig.ENDPOINTS, endpointsBackup);
  }

  /**
   * Get upstream from just signal endpoint Passing of this test implies that private methods
   * createUpstreamFromEndpoints upstreamFromNodes both work.
   *
   * @throws ApplicationException when it cannot parse json into upstream coonfig
   */
  @Test
  public void upstreamConfigFromNoEndpoint() throws ApplicationException {
    List<String> endpoints = sharedConfig.getEndpoints();

    assertNotNull(endpoints);
    assertTrue(endpoints.isEmpty());
    UpstreamConfig upstreamConfig = sharedConfig.getUpstreamConfig();
    assertNull(upstreamConfig);
  }

  /**
   * Get upstream from just signal endpoint Passing of this test implies that private methods
   * createUpstreamFromEndpoints upstreamFromNodes both work.
   *
   * @throws ApplicationException when it cannot parse json into upstream coonfig
   */
  @Test
  public void upstreamConfigFromSignalEndpoint() throws ApplicationException {
    List<String> endpoints = sharedConfig.getEndpoints();

    assertNotNull(endpoints);
    assertTrue(endpoints.isEmpty());

    sharedConfig.addEndpoint("http://172.18.0.11", "bios-signal", NodeType.SIGNAL);
    endpoints = sharedConfig.getEndpoints();

    assertEquals(1, endpoints.size());
    assertTrue(endpoints.contains("http://172.18.0.11"));

    UpstreamConfig upstreamConfig = sharedConfig.getUpstreamConfig();
    assertNotNull(upstreamConfig);
    assertNotNull(upstreamConfig.getUpstreams());
    assertNotNull(upstreamConfig.getUpstreams().get(0));
    assertEquals(upstreamConfig.getLbPolicy(), LbPolicy.ROUND_ROBIN);
    assertEquals(upstreamConfig.getUpstreams().size(), 1);

    Upstream upstream = upstreamConfig.getUpstreams().get(0);
    assertNotNull(upstream.getHostSet());
    assertNotNull(upstream.getHostSet()[0]);
    assertEquals(upstream.getHostSet()[0], "http://172.18.0.11");
    assertEquals(upstream.getHostSet().length, 1);
    assertNotNull(upstream.getOperationSet());

    OperationSet opSet = upstream.getOperationSet();
    assertNotNull(opSet.getOperation());
    assertNotNull(opSet.getOperation().get(0));
    assertEquals(opSet.getOperation().size(), 2);
    assertEquals(opSet.getOperation().get(0).getOperationType(), OperationType.INGEST);
    assertEquals(opSet.getOperation().get(0).getOperationPolicy(), OperationPolicy.ALWAYS);
    assertEquals(opSet.getOperation().get(1).getOperationType(), OperationType.CONTEXT_WRITE);
    assertEquals(opSet.getOperation().get(1).getOperationPolicy(), OperationPolicy.ALWAYS);

    String s = upstreamConfig.toString();
    assertFalse(s.isEmpty());

    sharedConfig.removeEndpoint("http://172.18.0.11", NodeType.SIGNAL);

    endpoints = sharedConfig.getEndpoints();
    assertEquals(0, endpoints.size());
  }

  /**
   * Get upstream from Analysis Endpoint.
   *
   * @throws ApplicationException when it cannot parse the default json
   */
  @Test
  public void upstreamConfigFromAnalysisEndpoint() throws ApplicationException {
    List<String> endpoints = sharedConfig.getEndpoints();

    assertNotNull(endpoints);
    assertTrue(endpoints.isEmpty());

    sharedConfig.addEndpoint("http://172.18.0.13", "bios-analysis", NodeType.ANALYSIS);
    endpoints = sharedConfig.getEndpoints();

    assertEquals(1, endpoints.size());
    assertTrue(endpoints.contains("http://172.18.0.13"));
    UpstreamConfig upstreamConfig = sharedConfig.getUpstreamConfig();
    assertNotNull(upstreamConfig);
    assertNotNull(upstreamConfig.getUpstreams());
    assertNotNull(upstreamConfig.getUpstreams().get(0));
    assertEquals(upstreamConfig.getLbPolicy(), LbPolicy.ROUND_ROBIN);
    assertEquals(upstreamConfig.getUpstreams().size(), 1);

    Upstream upstream = upstreamConfig.getUpstreams().get(0);
    assertNotNull(upstream.getHostSet());
    assertNotNull(upstream.getHostSet()[0]);
    assertEquals(upstream.getHostSet()[0], "http://172.18.0.13");
    assertEquals(upstream.getHostSet().length, 1);
    assertNotNull(upstream.getOperationSet());
    String s = upstream.toString();
    assertFalse(s.isEmpty());

    OperationSet opSet = upstream.getOperationSet();
    assertNotNull(opSet.getOperation());
    assertNotNull(opSet.getOperation().get(0));
    assertEquals(opSet.getOperation().size(), 3);
    assertEquals(opSet.getOperation().get(0).getOperationType(), OperationType.SUMMARIZE);
    assertEquals(opSet.getOperation().get(0).getOperationPolicy(), OperationPolicy.ALWAYS);
    assertEquals(opSet.getOperation().get(1).getOperationType(), OperationType.ADMIN_READ);
    assertEquals(opSet.getOperation().get(1).getOperationPolicy(), OperationPolicy.ALWAYS);
    assertEquals(opSet.getOperation().get(2).getOperationType(), OperationType.ADMIN_WRITE);
    assertEquals(opSet.getOperation().get(2).getOperationPolicy(), OperationPolicy.ALWAYS);
    s = opSet.toString();
    assertFalse(s.isEmpty());
    sharedConfig.removeEndpoint("http://172.18.0.13", NodeType.ANALYSIS);

    endpoints = sharedConfig.getEndpoints();
    assertEquals(0, endpoints.size());
  }

  /**
   * get upstream from rollup endpoint.
   *
   * @throws ApplicationException when it cannot parse json into upstream config
   */
  @Test
  public void upstreamConfigFromRollupEndpoint() throws ApplicationException {
    List<String> endpoints = sharedConfig.getEndpoints();

    assertNotNull(endpoints);
    assertTrue(endpoints.isEmpty());

    sharedConfig.addEndpoint("http://172.18.0.12", "bios-rollup", NodeType.ROLLUP);
    endpoints = sharedConfig.getEndpoints();

    assertEquals(1, endpoints.size());
    assertTrue(endpoints.contains("http://172.18.0.12"));
    UpstreamConfig upstreamConfig = sharedConfig.getUpstreamConfig();
    assertNotNull(upstreamConfig);
    assertNotNull(upstreamConfig.getUpstreams());
    assertNotNull(upstreamConfig.getUpstreams().get(0));
    assertEquals(upstreamConfig.getLbPolicy(), LbPolicy.ROUND_ROBIN);
    assertEquals(upstreamConfig.getUpstreams().size(), 1);

    Upstream upstream = upstreamConfig.getUpstreams().get(0);
    assertNotNull(upstream.getHostSet());
    assertNotNull(upstream.getHostSet()[0]);
    assertEquals(upstream.getHostSet()[0], "http://172.18.0.12");
    assertEquals(upstream.getHostSet().length, 1);
    assertNotNull(upstream.getOperationSet());

    OperationSet opSet = upstream.getOperationSet();
    assertNotNull(opSet.getOperation());
    assertNotNull(opSet.getOperation().get(0));
    assertEquals(opSet.getOperation().size(), 1);
    assertEquals(opSet.getOperation().get(0).getOperationType(), OperationType.EXTRACT);
    assertEquals(opSet.getOperation().get(0).getOperationPolicy(), OperationPolicy.ALWAYS);

    sharedConfig.removeEndpoint("http://172.18.0.12", NodeType.ROLLUP);
    endpoints = sharedConfig.getEndpoints();
    assertEquals(0, endpoints.size());
  }

  /**
   * Get upstreamConfig from three endpoints This alone is sufficient to test all three of the above
   * functions but this test relies on the endpoints being particular order. If the order is
   * changed, the the test will fail. So if the above three pass and this one fails, look at the
   * order in the upstreamConfig object and the test. There should be a way to test without order.
   *
   * @throws ApplicationException when it cannot parse the JSON into upstream config.
   */
  @Test
  public void upstreamConfigFromThreeEndpoints() throws ApplicationException {
    List<String> endpoints = sharedConfig.getEndpoints();

    assertNotNull(endpoints);
    assertTrue(endpoints.isEmpty());

    sharedConfig.addEndpoint("http://172.18.0.11", "bios-signal", NodeType.SIGNAL);
    sharedConfig.addEndpoint("http://172.18.0.12", "bios-rollup", NodeType.ROLLUP);
    sharedConfig.addEndpoint("http://172.18.0.13", "bios-analysis", NodeType.ANALYSIS);
    endpoints = sharedConfig.getEndpoints();

    assertEquals(3, endpoints.size());
    assertTrue(endpoints.contains("http://172.18.0.11"));
    assertTrue(endpoints.contains("http://172.18.0.12"));
    assertTrue(endpoints.contains("http://172.18.0.13"));
    UpstreamConfig upstreamConfig = sharedConfig.getUpstreamConfig();
    assertNotNull(upstreamConfig);
    assertNotNull(upstreamConfig.getUpstreams());
    assertNotNull(upstreamConfig.getUpstreams().get(0));
    assertEquals(upstreamConfig.getLbPolicy(), LbPolicy.ROUND_ROBIN);
    assertEquals(upstreamConfig.getUpstreams().size(), 3);

    Upstream upstream = upstreamConfig.getUpstreams().get(0);
    assertNotNull(upstream.getHostSet());
    assertNotNull(upstream.getHostSet()[0]);
    assertEquals(upstream.getHostSet()[0], "http://172.18.0.11");
    assertEquals(upstream.getHostSet().length, 1);
    assertNotNull(upstream.getOperationSet());

    OperationSet opSet = upstream.getOperationSet();
    assertNotNull(opSet.getOperation());
    assertNotNull(opSet.getOperation().get(0));
    assertEquals(opSet.getOperation().size(), 2);
    assertEquals(opSet.getOperation().get(0).getOperationType(), OperationType.INGEST);
    assertEquals(opSet.getOperation().get(0).getOperationPolicy(), OperationPolicy.ALWAYS);
    assertEquals(opSet.getOperation().get(1).getOperationType(), OperationType.CONTEXT_WRITE);
    assertEquals(opSet.getOperation().get(1).getOperationPolicy(), OperationPolicy.ALWAYS);

    upstream = upstreamConfig.getUpstreams().get(1);
    assertNotNull(upstream.getHostSet());
    assertNotNull(upstream.getHostSet()[0]);
    assertEquals(upstream.getHostSet()[0], "http://172.18.0.12");
    assertEquals(upstream.getHostSet().length, 1);
    assertNotNull(upstream.getOperationSet());

    opSet = upstream.getOperationSet();
    assertNotNull(opSet.getOperation());
    assertNotNull(opSet.getOperation().get(0));
    assertEquals(opSet.getOperation().size(), 1);
    assertEquals(opSet.getOperation().get(0).getOperationType(), OperationType.EXTRACT);
    assertEquals(opSet.getOperation().get(0).getOperationPolicy(), OperationPolicy.ALWAYS);

    upstream = upstreamConfig.getUpstreams().get(2);
    assertNotNull(upstream.getHostSet());
    assertNotNull(upstream.getHostSet()[0]);
    assertEquals(upstream.getHostSet()[0], "http://172.18.0.13");
    assertEquals(upstream.getHostSet().length, 1);
    assertNotNull(upstream.getOperationSet());

    opSet = upstream.getOperationSet();
    assertNotNull(opSet.getOperation());
    assertNotNull(opSet.getOperation().get(0));
    assertEquals(opSet.getOperation().size(), 3);
    assertEquals(opSet.getOperation().get(0).getOperationType(), OperationType.SUMMARIZE);
    assertEquals(opSet.getOperation().get(0).getOperationPolicy(), OperationPolicy.ALWAYS);
    assertEquals(opSet.getOperation().get(1).getOperationType(), OperationType.ADMIN_READ);
    assertEquals(opSet.getOperation().get(1).getOperationPolicy(), OperationPolicy.ALWAYS);
    assertEquals(opSet.getOperation().get(2).getOperationType(), OperationType.ADMIN_WRITE);
    assertEquals(opSet.getOperation().get(2).getOperationPolicy(), OperationPolicy.ALWAYS);

    sharedConfig.removeEndpoint("http://172.18.0.12", NodeType.ROLLUP);
    endpoints = sharedConfig.getEndpoints();

    assertEquals(2, endpoints.size());
    assertTrue(endpoints.contains("http://172.18.0.11"));
    assertTrue(endpoints.contains("http://172.18.0.13"));

    sharedConfig.removeEndpoint("http://172.18.0.11", NodeType.SIGNAL);
    sharedConfig.removeEndpoint("http://172.18.0.13", NodeType.ANALYSIS);

    endpoints = sharedConfig.getEndpoints();
    assertEquals(0, endpoints.size());
  }
}
