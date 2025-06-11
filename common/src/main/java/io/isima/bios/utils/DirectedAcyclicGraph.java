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
package io.isima.bios.utils;

import io.isima.bios.exceptions.DirectedAcyclicGraphException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * For maintaining a dependency order between nodes, just with names of the nodes. Uses a variation
 * of Kahn's topological sort algorithm to return the nodes in dependency order.
 *
 * <p>Just uses node names and gradually learns about dependencies through the {@link
 * this#addDirectedEdge(String, String)} method. It is assumed that the edges that define the
 * dependencies are persisted outside this class.
 *
 * <p>While this algorithm is currently used to determine dependency order between streams, this can
 * be used to determine dependency between any nodes that can be represented as strings and return
 * the least dependent node first and so on.
 */
public final class DirectedAcyclicGraph implements Cloneable {
  private final Map<String, Set<String>> childrenMap;
  private final Set<String> allNodes;

  public DirectedAcyclicGraph() {
    this.childrenMap = new ConcurrentHashMap<>();
    this.allNodes = ConcurrentHashMap.newKeySet();
  }

  /**
   * Adds a directed edge from a parent to a child if it does not cause a cycle. Also adds both
   * parent and child to the set of nodes in this graph.
   */
  public void addDirectedEdge(String parent, String child) throws DirectedAcyclicGraphException {

    // First check whether adding this edge will cause a cycle in this directed graph.
    if ((allNodes.contains(parent)) && allNodes.contains(child)) {
      // Currently this directed graph is acyclic. Adding this edge will cause a cycle to be formed
      // if and only if "parent" is currently a descendant of "child".
      if (isDescendantOf(parent, child)) {
        throw new DirectedAcyclicGraphException(
            String.format(
                "%s is currently a descendant of %s; adding this edge will cause a cycle.",
                parent, child));
      }
    }
    // Add the edge by adding this child to the parent's children set.
    childrenMap.compute(
        parent,
        (k, v) -> {
          if (v == null) {
            return new HashSet<>(Collections.singleton(child));
          } else {
            v.add(child);
            return v;
          }
        });
    // Add both parent and child to the set of nodes in this graph.
    allNodes.add(parent);
    allNodes.add(child);
  }

  /**
   * Remove this node and all its incoming edges from the DAG; throws exception if it has any
   * outgoing edges.
   */
  public void deleteNodeIfNoChildren(String node) throws DirectedAcyclicGraphException {
    // If this node is not being tracked by this DAG currently, just return.
    if (!allNodes.contains(node)) {
      return;
    }

    // Check whether it has any children.
    final var children = childrenMap.get(node);
    if (children != null) {
      assert (children.size() > 0);
      throw new DirectedAcyclicGraphException(
          String.format("Cannot delete node %s because it has children: %s.", node, children));
    }

    // Remove all incoming edges to this node.
    childrenMap
        .keySet()
        .forEach(
            (parent) -> {
              childrenMap.compute(
                  parent,
                  (k, v) -> {
                    if (v == null) {
                      return null;
                    } else {
                      v.remove(node);
                      if (v.isEmpty()) {
                        return null;
                      } else {
                        return v;
                      }
                    }
                  });
            });
    // Remove this node from the set of all nodes tracked by this DAG.
    allNodes.remove(node);
  }

  public void clearGraph() {
    childrenMap.clear();
    allNodes.clear();
  }

  public List<DirectedEdge> getAllDirectedEdges() {
    return childrenMap.entrySet().stream()
        .flatMap((e) -> e.getValue().stream().map((x) -> new DirectedEdge(e.getKey(), x)))
        .collect(Collectors.toUnmodifiableList());
  }

  public boolean nodeExists(String node) {
    return allNodes.contains(node);
  }

  /** Returns the set of direct children of a given node; empty set if no children exist. */
  public Set<String> getChildren(String node) {
    final var children = childrenMap.get(node);
    return (children != null)
        ? children.stream().collect(Collectors.toUnmodifiableSet())
        : Collections.emptySet();
  }

  /**
   * Returns all the transitive descendants of a give node.
   *
   * @param node The node whose descendants need to be returned.
   * @return Set of descendants of this node; returns empty set if node does not belong to this
   *     graph or if it does not have any descendants.
   */
  public Set<String> getDescendants(String node) {
    Set<String> descendants = new HashSet<>();
    if (!allNodes.contains(node)) {
      return descendants;
    }
    // To get all descendants, do a transitive traversal of the children sets.
    // Start with the current node as the node to be visited.
    Set<String> visited = new HashSet<>();
    Queue<String> toBeVisited = new ArrayDeque<String>();
    toBeVisited.add(node);
    while (!toBeVisited.isEmpty()) {
      // When you visit a node:
      //  Remove it from toBeVisited and mark it as visited.
      //  Add all its children to descendants set.
      //  If we have not already visited a child, add it to the toBeVisited set.
      final String currentNode = toBeVisited.poll();
      visited.add(currentNode);

      final var children = childrenMap.get(currentNode);
      if (children == null) {
        continue;
      }
      for (final var child : children) {
        descendants.add(child);
        if (!visited.contains(child)) {
          toBeVisited.add(child);
        }
      }
    }
    return descendants;
  }

  /**
   * Checks whether the given node potentialDescendant is a descendant of node potentialAncestor.
   */
  public boolean isDescendantOf(String potentialDescendant, String potentialAncestor) {
    // Given that we anyway need to implement getDescendants(), the simplest way to implement
    // isDescendantOf is to get the descendants and check if the node to check is contained in that
    // set. A potentially faster approach would be to use the same algorithm as getDescendants(),
    // but jump out immediately if we see that the node to be checked is found to be a descendant.
    // Our current use cases mostly expect isDescendantOf() to return false and in that case there
    // is no difference in performance, so we are using the simpler approach.
    final var descendants = getDescendants(potentialAncestor);
    return descendants.contains(potentialDescendant);
  }

  /**
   * Append nodes in topological order (parents come before children).
   *
   * <p>@see <a href="https://en.wikipedia.org/wiki/Topological_sorting#Kahn's_algorithm">Kahn's
   * algorithm </a> It is safe to use this algorithm because we enforce that no cycles exist in this
   * graph.
   *
   * @param targetList list to append the nodes to.
   */
  public void appendNodesInTopologicalOrder(List<String> targetList) {
    Objects.requireNonNull(targetList, "Target list must be non null");
    if (allNodes.isEmpty() || childrenMap.isEmpty()) {
      return;
    }

    // Initialize the inDegree map by counting incoming edges for each node.
    final Map<String, Integer> inDegreeMap = new HashMap<>(allNodes.size());
    for (final var node : allNodes) {
      inDegreeMap.put(node, 0);
    }
    for (final var children : childrenMap.values()) {
      for (final var child : children) {
        inDegreeMap.compute(
            child,
            (k, v) -> {
              return v + 1;
            });
      }
    }

    // In Kahn's algorithm, it is acceptable to consume nodes from readyNodes in any order - it is
    // supposed to be a set. However, we prefer to use a consistent ordering to get deterministic
    // results and avoid unexpected behavior changes. Using a PriorityQueue will result in a
    // topological ordering with lowest-available-vertex-first.
    final Queue<String> readyNodes = new PriorityQueue<>();
    final Deque<String> orderedNodes = new ArrayDeque<>();

    for (final var entry : inDegreeMap.entrySet()) {
      if (entry.getValue() == 0) {
        readyNodes.add(entry.getKey());
      }
    }

    while (!readyNodes.isEmpty()) {
      final var readyNode = readyNodes.poll();
      orderedNodes.addLast(readyNode);
      final var children = childrenMap.get(readyNode);
      if (children != null) {
        for (final var child : children) {
          inDegreeMap.compute(
              child,
              (k, v) -> {
                assert (v > 0);
                if (--v == 0) {
                  readyNodes.add(child);
                }
                return v;
              });
        }
      }
    }

    // Check if there are any nodes with inDegree > 0. If so, it indicates a cycle in the graph.
    for (final var entry : inDegreeMap.entrySet()) {
      assert (entry.getValue() >= 0) : "Bug: residual inDegree cannot be negative.";
      assert (entry.getValue() == 0) : "Bug: Looks like this graph has a cycle.";
    }

    orderedNodes.iterator().forEachRemaining(targetList::add);
  }

  @Override
  public DirectedAcyclicGraph clone() {
    final var clone = new DirectedAcyclicGraph();

    // Make a deep copy of all the nodes and adjacency sets.
    allNodes.forEach((node) -> clone.allNodes.add(node));

    childrenMap.forEach(
        (String parent, Set<String> children) -> {
          final var newChildren = new HashSet<String>();
          children.forEach((child) -> newChildren.add(child));
          clone.childrenMap.put(parent, newChildren);
        });

    return clone;
  }
}
