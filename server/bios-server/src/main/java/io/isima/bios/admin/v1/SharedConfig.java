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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import io.isima.bios.common.BiosConstants;
import io.isima.bios.common.SharedProperties;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.ExecutorManager;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.models.NodeType;
import io.isima.bios.models.OperationSet;
import io.isima.bios.models.Upstream;
import io.isima.bios.models.UpstreamConfig;
import io.isima.bios.utils.TfosObjectMapperProvider;
import io.isima.bios.utils.Utils;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that provides TFOS configuration that is shared among multiple service nodes.
 * SharedProperties is a basic key, value store and this class builds upon it and adds more
 * structure e.g. delimited entries within a string value.
 */
public class SharedConfig {
  private static final Logger logger = LoggerFactory.getLogger(SharedConfig.class);

  public static final String ENDPOINTS_DELIMITER = "\n";
  public static final String NODES_DELIMITER = ENDPOINTS_DELIMITER;
  public static final String NODE_INFO_SEPARATOR = ":";

  // configuration keys
  public static final String NODES = "nodes";
  public static final String UPSTREAM = "upstream";
  public static final String ENDPOINTS = "endpoints";
  public static final String FAILED_ENDPOINTS = "failed_endpoints";

  public static final String PREFIX_ENDPOINT = "endpoint:";
  public static final String PREFIX_NODE = "node:";

  public static final String SESSION_EXPIRATION_MILLIS = "session_expiry";

  private static final String PREFIX_TENANT = "tenant.";

  @Getter private final SharedProperties sharedProperties;
  private final ObjectMapper mapper = TfosObjectMapperProvider.get();
  private static final String JSON_FILE = "nodeListToUpstream.json";
  private MapType mapType =
      mapper.getTypeFactory().constructMapType(Map.class, String.class, OperationSet.class);
  private Map<String, OperationSet> strToOS = null;

  private String hostName = null;
  private NodeType nodeType = null;
  private long nodeTypeExpiryLoadTime = 0L;
  private final long nodeTypeExpiryMilliseconds = 1000L * 60 * 30;

  /**
   * The constructor.
   *
   * @param sharedProperties shared property instance to be used
   */
  public SharedConfig(SharedProperties sharedProperties) throws ApplicationException {
    this.sharedProperties = sharedProperties;
    try (InputStream inputStream =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(JSON_FILE)) {
      this.strToOS = mapper.readValue(inputStream, this.mapType);
    } catch (IOException e) {
      throw new ApplicationException("Could not load template upstream from " + JSON_FILE, e);
    }
  }

  /**
   * Returns list of server endpoints.
   *
   * <p>The method fetches the registered endpoints and failed endpoints and returns the names. In
   * case of failed node, the returned name would be prefixed by period ('.'), such as
   * ".https://node3:8443".
   *
   * @return Server endpoints as a list of string. Endpoint names of failed nodes are prefixed by
   *     period.
   */
  public List<String> getEndpoints() {
    final String nodes = sharedProperties.getProperty(NODES);
    return parseNodes(nodes);
  }

  public CompletionStage<List<String>> getEndpointsAsync(ExecutionState state) {
    final var future = new CompletableFuture<List<String>>();

    sharedProperties.getPropertyCachedAsync(
        NODES,
        null,
        state,
        (value) -> {
          parseNodesAsync(value, state, future::complete, future::completeExceptionally);
        },
        future::completeExceptionally);

    return future;
  }

  public CompletionStage<List<String>> getRemoteEndpoints(ExecutionState state) {
    final var future = new CompletableFuture<List<String>>();
    sharedProperties.getPropertyCachedAsync(
        NODES,
        null,
        state,
        (value) -> {
          final var endpoints = parseNodes(value);
          sharedProperties.getLocalEndpointAsync(
              endpoints,
              state,
              (localEndpoint) -> {
                final var remoteEndpoints =
                    endpoints.stream()
                        .filter((endpoint) -> !endpoint.equals(localEndpoint))
                        .collect(Collectors.toList());
                future.complete(remoteEndpoints);
              },
              future::completeExceptionally);
        },
        future::completeExceptionally);
    return future;
  }

  private List<String> parseNodes(String nodes) {
    if (nodes == null || nodes.isEmpty()) {
      return Collections.emptyList();
    }
    final List<String> endPoints = new ArrayList<>();
    for (String node : nodes.split(NODES_DELIMITER)) {
      final String[] temp = node.split(NODE_INFO_SEPARATOR, 2);
      if (temp.length == 2) {
        endPoints.add(temp[1]);
      }
    }
    return getFinalEndpoints(endPoints);
  }

  private void parseNodesAsync(
      String nodes,
      ExecutionState state,
      Consumer<List<String>> acceptor,
      Consumer<Throwable> errorHandler) {
    if (nodes == null || nodes.isEmpty()) {
      acceptor.accept(List.of());
      return;
    }
    final List<String> endPoints = new ArrayList<>();
    for (String node : nodes.split(NODES_DELIMITER)) {
      final String[] temp = node.split(NODE_INFO_SEPARATOR, 2);
      if (temp.length == 2) {
        endPoints.add(temp[1]);
      }
    }
    getFinalEndpointsAsync(endPoints, state, acceptor, errorHandler);
  }

  private Upstream upstreamFromNodes(String type, String node) throws ApplicationException {
    final Upstream upstream = new Upstream();
    final String[] hostset = new String[1];
    OperationSet operationSet = strToOS.get(type);
    hostset[0] = node;
    upstream.setHostSet(hostset);
    upstream.setOperationSet(operationSet);
    return upstream;
  }

  private UpstreamConfig createUpstreamFromEndpoints(String nodes) throws ApplicationException {
    // This is a placeholder till I figureout how to create upstream json from
    // endpoint list.
    String[] endpoints = nodes.split(NODES_DELIMITER);
    UpstreamConfig upstreamConfig = new UpstreamConfig();
    List<Upstream> upstreams = new ArrayList<>();
    for (int i = 0; i < endpoints.length; i++) {
      final String[] temp = endpoints[i].split(NODE_INFO_SEPARATOR, 2);
      upstreams.add(upstreamFromNodes(temp[0], temp[1]));
    }
    upstreamConfig.setUpstreams(upstreams);
    return upstreamConfig;
  }

  /**
   * Returns Upstream Config JSON string.
   *
   * <p>Fetches upstream config from cassandra as mentioned in
   * https://elasticflash.atlassian.net/wiki/spaces/TFOS/pages/933986306/Design+of+CSDK+Load+Balancing+Feature
   *
   * @return upstream config JSON string.
   * @throws ApplicationException when it cannot parse JSON into object
   */
  public UpstreamConfig getUpstreamConfig() throws ApplicationException {
    final String upStreamConfig = sharedProperties.getProperty(UPSTREAM);
    if (upStreamConfig == null || upStreamConfig.isEmpty()) {
      final String nodes = sharedProperties.getProperty(NODES);
      if (nodes == null || nodes.isEmpty()) {
        return null;
      }
      return createUpstreamFromEndpoints(nodes);
    }
    try {
      return mapper.readValue(upStreamConfig, UpstreamConfig.class);

    } catch (JsonParseException e) {
      throw new ApplicationException(
          "Could not convert endpoint to upstream: " + upStreamConfig, e);
    } catch (JsonMappingException e) {
      throw new ApplicationException("Could not map all fields of json:  " + upStreamConfig, e);
    } catch (IOException e) {
      throw new ApplicationException("Could not read json for" + upStreamConfig, e);
    }
  }

  public CompletableFuture<UpstreamConfig> getUpstreamConfigAsync(ExecutionState state) {
    return sharedProperties
        .getPropertyAsync(UPSTREAM, state)
        .thenCompose(
            (upstreamConfig) -> {
              if (upstreamConfig == null || upstreamConfig.isEmpty()) {
                return sharedProperties
                    .getPropertyAsync(NODES, state)
                    .thenCompose(
                        (nodes) -> {
                          if (nodes == null || nodes.isEmpty()) {
                            return CompletableFuture.completedFuture(null);
                          }
                          return CompletableFuture.supplyAsync(
                              () -> {
                                try {
                                  return createUpstreamFromEndpoints(nodes);
                                } catch (ApplicationException e) {
                                  throw new CompletionException(e);
                                }
                              },
                              ExecutorManager.getSidelineExecutor());
                        });
              }
              try {
                return CompletableFuture.completedFuture(
                    mapper.readValue(upstreamConfig, UpstreamConfig.class));
              } catch (JsonParseException e) {
                throw new CompletionException(
                    new ApplicationException(
                        "Could not convert endpoint to upstream: " + upstreamConfig, e));
              } catch (JsonMappingException e) {
                throw new CompletionException(
                    new ApplicationException(
                        "Could not map all fields of json:  " + upstreamConfig, e));
              } catch (IOException e) {
                throw new CompletionException(
                    new ApplicationException("Could not read json for" + upstreamConfig, e));
              }
            });
  }

  /**
   * Returns list of server endpoints which contains context cache.
   *
   * <p>The method fetches the registered endpoints and failed endpoints and returns the names. In
   * case of failed node, the returned name would be prefixed by period ('.'), such as
   * ".https://node3:8443".
   *
   * @return Server endpoints as a list of string. Endpoint names of failed nodes are prefixed by
   *     period.
   */
  public List<String> getContextEndpoints() {
    final String nodes = sharedProperties.getProperty(NODES);
    if (nodes == null || nodes.isEmpty()) {
      return Collections.emptyList();
    }

    final List<String> endPoints = new ArrayList<>();
    for (String node : nodes.split(NODES_DELIMITER)) {
      final String[] temp = node.split(NODE_INFO_SEPARATOR, 2);
      final NodeType nodeType = NodeType.forValue(temp[0]);

      if (temp.length == 2 && NodeType.SIGNAL.equals(nodeType)) {
        endPoints.add(temp[1]);
      }
    }
    return getFinalEndpoints(endPoints);
  }

  private List<String> getFinalEndpoints(final List<String> endPoints) {
    assert !ExecutorManager.isInIoThread();
    if (endPoints == null || endPoints.isEmpty()) {
      return Collections.emptyList();
    } else {
      // Add a prefix ('.') to all failed endpoints

      final List<String> resultEndpoints = new ArrayList<>();

      final var failed = getFailedEndpoints();

      for (String endpoint : endPoints) {
        if (failed.contains(endpoint)) {
          resultEndpoints.add(BiosConstants.PREFIX_FAILED_NODE_NAME + endpoint);
        } else {
          resultEndpoints.add(endpoint);
        }
      }
      return resultEndpoints;
    }
  }

  private void getFinalEndpointsAsync(
      final List<String> endPoints,
      ExecutionState state,
      Consumer<List<String>> acceptor,
      Consumer<Throwable> errorHandler) {
    if (endPoints == null || endPoints.isEmpty()) {
      acceptor.accept(List.of());
      return;
    }

    // Add a prefix ('.') to all failed endpoints
    sharedProperties.getPropertyAsync(
        FAILED_ENDPOINTS,
        state,
        (failedEndpointsSrc) -> {
          final var failedEndpoints =
              failedEndpointsSrc != null
                  ? Arrays.asList(failedEndpointsSrc.trim().split(ENDPOINTS_DELIMITER))
                  : List.of();

          final var resultEndpoints =
              endPoints.stream()
                  .map(
                      (endpoint) -> {
                        if (failedEndpoints.contains(endpoint)) {
                          return BiosConstants.PREFIX_FAILED_NODE_NAME + endpoint;
                        }
                        return endpoint;
                      })
                  .collect(Collectors.toList());

          acceptor.accept(resultEndpoints);
        },
        errorHandler::accept);
  }

  /**
   * Method to register a node.
   *
   * <p>The method adds an entry to the endpoint list. It also adds an entry to the node name
   * resolver.
   *
   * @param endpoint Server endpoint.
   * @param nodeName Server node name.
   * @param nodeType Type of Node.
   * @throws ApplicationException When an unexpected error happens.
   */
  public void registerNode(String endpoint, String nodeName, NodeType nodeType)
      throws ApplicationException {
    // check if node is already registered.
    final String nodes = sharedProperties.getProperty(NODES);
    if (StringUtils.containsIgnoreCase(nodes, endpoint)) {
      return;
    }

    addEndpoint(endpoint, nodeName, nodeType);
  }

  /**
   * Method to unregister a node.
   *
   * @param endpoint Endpoint of the unregistering node.
   * @throws ApplicationException When an unexpected error happens.
   */
  public void unregisterNode(String endpoint) throws ApplicationException {
    final String nodes = sharedProperties.getProperty(NODES);
    if (!StringUtils.containsIgnoreCase(nodes, endpoint)) {
      return;
    }

    // It's not required to pass its Type to unregister a Node.
    // Get the NodeType already registered for the endpoint.
    for (NodeType nodeType : NodeType.values()) {
      final String node = nodeType.getTypeName() + NODE_INFO_SEPARATOR + endpoint.trim();
      if (StringUtils.containsIgnoreCase(nodes, node)) {
        removeEndpoint(endpoint, nodeType);
        break;
      }
    }
  }

  /**
   * Adds a server endpoint entry. Also adds node name to endpoint (and reverse) lookup.
   *
   * @param endpoint the endpoint URL to add.
   * @param nodeName the node name
   * @param nodeType Type of Node.
   * @throws ApplicationException when writing to shared repository failed.
   */
  public void addEndpoint(String endpoint, String nodeName, NodeType nodeType)
      throws ApplicationException {
    if (endpoint == null) {
      throw new IllegalArgumentException("endpoint may not be null");
    }
    if (nodeName == null) {
      throw new IllegalArgumentException("nodeName may not be null");
    }
    if (nodeType == null) {
      throw new IllegalArgumentException("nodeType may not be null");
    }

    final String endpointEntry = nodeType.getTypeName() + NODE_INFO_SEPARATOR + endpoint.trim();
    sharedProperties.appendPropertyIfAbsent(NODES, endpointEntry, NODES_DELIMITER, true);
    sharedProperties.setProperty(PREFIX_NODE + nodeName, endpointEntry);

    final String endpointKey = PREFIX_ENDPOINT + endpoint;
    sharedProperties.setProperty(endpointKey, nodeName);
  }

  /**
   * Removes an entry from list of server endpoints.
   *
   * @param endpoint the endpoint URL to remove.
   * @param nodeType Type of Node.
   * @throws ApplicationException when writing to shared repository failed.
   */
  public void removeEndpoint(String endpoint, NodeType nodeType) throws ApplicationException {
    if (endpoint == null) {
      throw new IllegalArgumentException("endpoint may not be null");
    }
    if (nodeType == null) {
      throw new IllegalArgumentException("nodeType may not be null");
    }

    final String node = nodeType.getTypeName() + NODE_INFO_SEPARATOR + endpoint.trim();
    sharedProperties.removeProperty(NODES, node, NODES_DELIMITER, true);

    final String endpointKey = PREFIX_ENDPOINT + endpoint;

    // resolve the node name before removing the lookup entry
    final var nodeName = sharedProperties.getProperty(endpointKey);

    // remove the entry for endpoint to node lookup
    sharedProperties.setProperty(endpointKey, null);

    // remove the entry for node to endpoint lookup
    if (!StringUtils.isBlank(nodeName)) {
      sharedProperties.setProperty(PREFIX_NODE + nodeName, null);
    }
  }

  /**
   * Method to resolve a node name by endpoint URL.
   *
   * @param endpoint The endpoint URL.
   * @return Resolved node. An empty string is returned when the resolving was unsuccessful.
   */
  public String getNodeName(String endpoint) {
    if (endpoint == null) {
      throw new IllegalArgumentException("endpoint may not be null");
    }
    return sharedProperties.getProperty(PREFIX_ENDPOINT + endpoint);
  }

  /**
   * Gets endpoint by node name.
   *
   * @param nodeName the node name
   * @return Pair of node type and endpoint. Null is returned when the endpoint is not found.
   */
  public Pair<NodeType, String> nodeNameToEndpoint(String nodeName) {
    final var src = sharedProperties.getProperty(PREFIX_NODE + Objects.requireNonNull(nodeName));
    return parseEndpoint(nodeName, src);
  }

  public CompletionStage<Pair<NodeType, String>> nodeNameToEndpointAsync(
      String nodeName, ExecutionState state) {
    return sharedProperties
        .getPropertyAsync(PREFIX_NODE + Objects.requireNonNull(nodeName), state)
        .thenApply((src) -> parseEndpoint(nodeName, src));
  }

  private Pair<NodeType, String> parseEndpoint(String nodeName, String src) {
    if (src == null) {
      return null;
    }
    final var elements = src.split(":", 2);
    if (elements.length < 2) {
      return null;
    }
    try {
      final var nodeType = NodeType.valueOf(elements[0].toUpperCase());
      final var endpoint = elements[1];
      return Pair.of(nodeType, endpoint);
    } catch (IllegalArgumentException e) {
      logger.error(String.format("Failed to parse endpoint for node %s: %s", nodeName, src));
      return null;
    }
  }

  /**
   * Adds an entry to list of failed server endpoints.
   *
   * @param endpoint the endpoint URL to add.
   * @throws ApplicationException when writing to shared repository failed.
   */
  public void addFailedEndpoint(String endpoint) throws ApplicationException {
    sharedProperties.appendPropertyIfAbsent(
        FAILED_ENDPOINTS, endpoint.trim(), ENDPOINTS_DELIMITER, true);
  }

  /**
   * Removes an entry from list of failed server endpoints.
   *
   * @param endpoint the endpoint URL to remove.
   * @throws ApplicationException when writing to shared repository failed.
   */
  public void removeFailedEndpoint(String endpoint) throws ApplicationException {
    sharedProperties.removeProperty(FAILED_ENDPOINTS, endpoint.trim(), ENDPOINTS_DELIMITER, true);
  }

  /**
   * Gets list of failed endpoints.
   *
   * @return List of endpoints that are marked failure.
   */
  public Set<String> getFailedEndpoints() {
    final String failedEndpointsSrc = sharedProperties.getProperty(FAILED_ENDPOINTS);
    return StringUtils.isBlank(failedEndpointsSrc)
        ? Set.of()
        : Set.of(failedEndpointsSrc.trim().split(ENDPOINTS_DELIMITER));
  }

  /**
   * Method to get NodeType of server.
   *
   * @return NodeType enum
   */
  public NodeType getNodeType() {
    final long currentTime = System.currentTimeMillis();

    if ((nodeType == null) || (currentTime > nodeTypeExpiryLoadTime + nodeTypeExpiryMilliseconds)) {
      calculateNodeType();
      nodeTypeExpiryLoadTime = currentTime;
    }
    return nodeType;
  }

  private void calculateNodeType() {
    assert !ExecutorManager.isInIoThread();
    String nodeName = Utils.getNodeName();

    final String nodes = getProperty(NODES);
    if (nodes == null || nodes.isEmpty()) {
      return;
    }

    for (String node : nodes.split(NODES_DELIMITER)) {
      final String[] temp = node.split(NODE_INFO_SEPARATOR, 2);
      final NodeType nodeType = NodeType.forValue(temp[0]);
      if (nodeName.compareToIgnoreCase(getNodeName(temp[1])) == 0) {
        this.nodeType = nodeType;
      }
    }
  }

  /**
   * Generic method to get a shared property value.
   *
   * @param key Property key
   * @return Property value. An empty string is returned when the target entry was not found.
   */
  public String getProperty(String key) {
    assert !ExecutorManager.isInIoThread();
    if (key == null) {
      throw new IllegalArgumentException("key may not be null");
    }
    final String value = sharedProperties.getProperty(key);
    return value != null ? value : "";
  }

  /** Generic method to get a shared property value asynchronously. */
  public CompletionStage<String> getPropertyAsync(String key, ExecutionState state) {
    if (key == null) {
      throw new IllegalArgumentException("key may not be null");
    }
    return sharedProperties
        .getPropertyAsync(key, state)
        .thenApply((value) -> value != null ? value : "");
  }

  /**
   * Generic method to set a shared property value.
   *
   * @param key Property key
   * @param value Property value
   * @throws ApplicationException thrown to indicate an internal error happened.
   */
  public void setProperty(String key, String value) throws ApplicationException {
    Objects.requireNonNull(key, "key may not be null");
    sharedProperties.setProperty(key, value);
  }

  /** Method to migrate/converts endpoints information to nodes information. */
  public void migrateEndpointToNode() throws ApplicationException {
    final String existingNodes = sharedProperties.getProperty(NODES);
    if (StringUtils.isNotBlank(existingNodes)) {
      return;
    }

    // Need to convert endpoints to nodes.
    final String endpoints = sharedProperties.getProperty(ENDPOINTS);
    if (StringUtils.isBlank(endpoints)) {
      return;
    }

    final StringJoiner nodeJoiner = new StringJoiner(NODES_DELIMITER);

    for (String endpoint : endpoints.split(ENDPOINTS_DELIMITER)) {
      String node = NodeType.SIGNAL.getTypeName() + NODE_INFO_SEPARATOR + endpoint;
      nodeJoiner.add(node);
    }

    sharedProperties.setProperty(NODES, nodeJoiner.toString());
  }

  /**
   * Method to get session expiration milliseconds.
   *
   * <p>This method is used for testing.
   *
   * @return Expiration milliseconds or 0 if not specified.
   */
  public long getSessionExpirationMillis() {
    final String value = sharedProperties.getProperty(SESSION_EXPIRATION_MILLIS);
    try {
      return (value != null && !value.isBlank()) ? Long.parseLong(value) : 0;
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  public CompletionStage<Long> getSessionExpirationMillisAsync(ExecutionState state) {
    return sharedProperties
        .getPropertyAsync(SESSION_EXPIRATION_MILLIS, state)
        .thenApply(
            (value) -> {
              try {
                return (value != null && !value.isBlank()) ? Long.valueOf(value) : Long.valueOf(0);
              } catch (NumberFormatException e) {
                return Long.valueOf(0);
              }
            });
  }

  @Deprecated
  public String getTenantCached(
      String key, String tenantName, String defaultValueIfAbsent, boolean getClusterIfAbsent) {
    assert !ExecutorManager.isInIoThread();
    try {
      return getTenantCachedAsync(
              key,
              tenantName,
              defaultValueIfAbsent,
              getClusterIfAbsent,
              new GenericExecutionState("getTenantCached", ExecutorManager.getSidelineExecutor()))
          .get();
    } catch (InterruptedException e) {
      logger.error("Operation interrupted", e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      throw new RuntimeException(e.getCause());
    }
    throw new RuntimeException("Operation did not complete");
  }

  public CompletableFuture<String> getTenantCachedAsync(
      String key,
      String tenantName,
      String defaultValueIfAbsent,
      boolean getClusterIfAbsent,
      ExecutionState state) {
    final String tenantPropertyKey = PREFIX_TENANT + tenantName + "." + key;
    return sharedProperties
        .getPropertyCachedAsync(tenantPropertyKey, null, state)
        .thenCompose(
            (property) -> {
              if (property == null && getClusterIfAbsent) {
                return sharedProperties.getPropertyCachedAsync(key, defaultValueIfAbsent, state);
              }
              return CompletableFuture.completedFuture(
                  property != null ? property : defaultValueIfAbsent);
            });
  }
}
