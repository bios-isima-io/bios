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

import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.isima.bios.exceptions.DirectedAcyclicGraphException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Test;

public class DirectedGraphTest {
  // A -> B  ; result A, B
  @Test
  public void testSimpleDependency() throws Throwable {
    final var graphUnderTest = new DirectedAcyclicGraph();
    graphUnderTest.addDirectedEdge("A", "B");
    final var orderedNodes = new ArrayList<String>();
    graphUnderTest.appendNodesInTopologicalOrder(orderedNodes);
    assertThat(orderedNodes.size(), is(2));
    assertThat(orderedNodes.get(0), is("A"));
    assertThat(orderedNodes.get(1), is("B"));

    assertSetEquals(Arrays.asList("B"), graphUnderTest.getChildren("A"));
    assertSetEquals(Arrays.asList("B"), graphUnderTest.getDescendants("A"));
    assertSetEquals(Arrays.asList(), graphUnderTest.getChildren("B"));
    assertSetEquals(Arrays.asList(), graphUnderTest.getDescendants("B"));
    assertSetEquals(Arrays.asList(), graphUnderTest.getDescendants("nonExistentNode"));
    assertSetEquals(Arrays.asList(), graphUnderTest.getChildren("C"));
    assertSetEquals(Arrays.asList(), graphUnderTest.getDescendants("C"));

    assertTrue(graphUnderTest.isDescendantOf("B", "A"));
    assertFalse(graphUnderTest.isDescendantOf("A", "B"));
    assertFalse(graphUnderTest.isDescendantOf("C", "A"));

    assertThrows(
        DirectedAcyclicGraphException.class,
        () -> {
          graphUnderTest.deleteNodeIfNoChildren("A");
        });
    // This should not throw any exception.
    graphUnderTest.deleteNodeIfNoChildren("B");
    graphUnderTest.deleteNodeIfNoChildren("A");
  }

  // A -> B  ; result A, B
  // C -> D  ; result  first item must be A or C; last must be B or D
  @Test
  public void testSimpleDependencyMultiple() throws Throwable {
    final var graphUnderTest = new DirectedAcyclicGraph();
    graphUnderTest.addDirectedEdge("A", "B");
    graphUnderTest.addDirectedEdge("C", "D");
    final var orderedNodes = new ArrayList<String>();
    graphUnderTest.appendNodesInTopologicalOrder(orderedNodes);
    assertThat(orderedNodes.size(), is(4));
    assertThat(orderedNodes.get(0), either(is("A")).or(is("C")));
    assertThat(orderedNodes.get(3), either(is("B")).or(is("D")));

    assertSetEquals(Arrays.asList("B"), graphUnderTest.getDescendants("A"));
    assertSetEquals(Arrays.asList("D"), graphUnderTest.getDescendants("C"));
    assertSetEquals(Arrays.asList(), graphUnderTest.getDescendants("D"));
  }

  // A -> B
  // B -> C
  // C -> D
  // D -> E
  //
  // A -> B -> C -> D -> E
  @Test
  public void testSimpleChainDependency() throws Throwable {
    final var graphUnderTest = new DirectedAcyclicGraph();

    graphUnderTest.addDirectedEdge("D", "E");
    graphUnderTest.addDirectedEdge("A", "B");
    graphUnderTest.addDirectedEdge("C", "D");
    graphUnderTest.addDirectedEdge("B", "C");

    final var orderedNodes = new ArrayList<String>();
    graphUnderTest.appendNodesInTopologicalOrder(orderedNodes);
    assertThat(orderedNodes.size(), is(5));
    assertThat(orderedNodes.get(0), is("A"));
    assertThat(orderedNodes.get(1), is("B"));
    assertThat(orderedNodes.get(2), is("C"));
    assertThat(orderedNodes.get(3), is("D"));
    assertThat(orderedNodes.get(4), is("E"));

    assertSetEquals(Arrays.asList("B", "C", "D", "E"), graphUnderTest.getDescendants("A"));
    assertSetEquals(Arrays.asList("E", "D"), graphUnderTest.getDescendants("C"));
  }

  // B --> H
  // B -----> C
  // A -----> C ------> E
  // A ----------> D -> E -> F -> G
  // W -> X -> Y
  @Test
  public void testComplexChainDependency() throws Throwable {
    final var graphUnderTest = new DirectedAcyclicGraph();

    graphUnderTest.addDirectedEdge("A", "C");
    graphUnderTest.addDirectedEdge("A", "D");
    graphUnderTest.addDirectedEdge("B", "C");
    graphUnderTest.addDirectedEdge("C", "E");
    graphUnderTest.addDirectedEdge("B", "H");
    graphUnderTest.addDirectedEdge("F", "G");
    graphUnderTest.addDirectedEdge("D", "E");
    graphUnderTest.addDirectedEdge("E", "F");
    graphUnderTest.addDirectedEdge("W", "X");
    graphUnderTest.addDirectedEdge("X", "Y");

    verifyComplexChainDependency(graphUnderTest.clone());
    verifyComplexChainDependency(graphUnderTest);
  }

  private void verifyComplexChainDependency(final DirectedAcyclicGraph graphUnderTest)
      throws Throwable {
    Set<String> random = new HashSet<>(Arrays.asList("A", "L", "M", "B", "E", "G"));
    final var orderedNodes = new ArrayList<>(random);

    final int lOriginalIndex = orderedNodes.indexOf("L");
    final int mOriginalIndex = orderedNodes.indexOf("M");

    // Similar to pattern used in production code which has a mix of dependent and
    // non dependent nodes.
    orderedNodes.removeIf(graphUnderTest::nodeExists);
    graphUnderTest.appendNodesInTopologicalOrder(orderedNodes);

    final int aIndex = orderedNodes.indexOf("A");
    final int bIndex = orderedNodes.indexOf("B");
    final int cIndex = orderedNodes.indexOf("C");
    final int dIndex = orderedNodes.indexOf("D");
    final int eIndex = orderedNodes.indexOf("E");
    final int fIndex = orderedNodes.indexOf("F");
    final int gIndex = orderedNodes.indexOf("G");
    final int hIndex = orderedNodes.indexOf("H");
    final int yIndex = orderedNodes.indexOf("Y");
    final int xIndex = orderedNodes.indexOf("X");
    final int wIndex = orderedNodes.indexOf("W");
    final int lIndex = orderedNodes.indexOf("L");
    final int mIndex = orderedNodes.indexOf("M");
    final int qIndex = orderedNodes.indexOf("Q");

    assertThat(aIndex, lessThan(cIndex));
    assertThat(aIndex, lessThan(dIndex));
    assertThat(bIndex, lessThan(cIndex));
    assertThat(bIndex, lessThan(hIndex));
    assertThat(cIndex, lessThan(eIndex));
    assertThat(dIndex, lessThan(eIndex));
    assertThat(eIndex, lessThan(fIndex));
    assertThat(fIndex, lessThan(gIndex));
    assertThat(wIndex, lessThan(xIndex));
    assertThat(xIndex, lessThan(yIndex));

    if (lOriginalIndex < mOriginalIndex) {
      assertThat(lIndex, is(0));
      assertThat(mIndex, is(1));
    } else {
      assertThat(lIndex, is(1));
      assertThat(mIndex, is(0));
    }
    assertThat(qIndex, is(-1));

    assertSetEquals(Arrays.asList("C", "D"), graphUnderTest.getChildren("A"));
    assertSetEquals(Arrays.asList("C", "H"), graphUnderTest.getChildren("B"));
    assertSetEquals(Arrays.asList("E"), graphUnderTest.getChildren("C"));
    assertSetEquals(Arrays.asList("E"), graphUnderTest.getChildren("D"));
    assertSetEquals(Arrays.asList("F"), graphUnderTest.getChildren("E"));
    assertSetEquals(Arrays.asList("G"), graphUnderTest.getChildren("F"));
    assertSetEquals(Arrays.asList("X"), graphUnderTest.getChildren("W"));
    assertSetEquals(Arrays.asList("Y"), graphUnderTest.getChildren("X"));
    assertSetEquals(Arrays.asList(), graphUnderTest.getChildren("Y"));
    assertSetEquals(Arrays.asList(), graphUnderTest.getChildren("G"));
    assertSetEquals(Arrays.asList(), graphUnderTest.getChildren("H"));

    assertSetEquals(Arrays.asList("C", "D", "E", "F", "G"), graphUnderTest.getDescendants("A"));
    assertSetEquals(Arrays.asList("E", "F", "G"), graphUnderTest.getDescendants("C"));
    assertSetEquals(Arrays.asList("C", "E", "F", "G", "H"), graphUnderTest.getDescendants("B"));
    assertSetEquals(Arrays.asList("X", "Y"), graphUnderTest.getDescendants("W"));

    assertTrue(graphUnderTest.isDescendantOf("G", "A"));
    assertTrue(graphUnderTest.isDescendantOf("H", "B"));
    assertTrue(graphUnderTest.isDescendantOf("F", "C"));
    assertFalse(graphUnderTest.isDescendantOf("Y", "B"));
    assertFalse(graphUnderTest.isDescendantOf("H", "A"));
    assertFalse(graphUnderTest.isDescendantOf("D", "C"));

    assertAddEdgeThrowsDagException(graphUnderTest, "Y", "X");
    assertAddEdgeThrowsDagException(graphUnderTest, "Y", "W");
    assertAddEdgeThrowsDagException(graphUnderTest, "G", "A");
    assertAddEdgeThrowsDagException(graphUnderTest, "E", "B");
    assertAddEdgeThrowsDagException(graphUnderTest, "F", "B");
    assertAddEdgeThrowsDagException(graphUnderTest, "F", "E");

    assertDeleteNodeThrowsDagException(graphUnderTest, "B");
    assertDeleteNodeThrowsDagException(graphUnderTest, "C");
    assertDeleteNodeThrowsDagException(graphUnderTest, "A");
    assertDeleteNodeThrowsDagException(graphUnderTest, "D");
    assertDeleteNodeThrowsDagException(graphUnderTest, "E");
    assertDeleteNodeThrowsDagException(graphUnderTest, "F");
    assertDeleteNodeThrowsDagException(graphUnderTest, "W");
    assertDeleteNodeThrowsDagException(graphUnderTest, "X");
    // This sequnce of deletes should not throw any exception.
    graphUnderTest.deleteNodeIfNoChildren("G");
    graphUnderTest.deleteNodeIfNoChildren("Y");
    graphUnderTest.deleteNodeIfNoChildren("X");
    graphUnderTest.deleteNodeIfNoChildren("W");
    graphUnderTest.deleteNodeIfNoChildren("F");
    graphUnderTest.deleteNodeIfNoChildren("E");
    graphUnderTest.deleteNodeIfNoChildren("D");
    graphUnderTest.deleteNodeIfNoChildren("C");
    graphUnderTest.deleteNodeIfNoChildren("A");
    graphUnderTest.deleteNodeIfNoChildren("H");
    graphUnderTest.deleteNodeIfNoChildren("B");
  }

  @Test
  public void testCloneAndClearDependencies() throws Throwable {
    final var graphUnderTest = new DirectedAcyclicGraph();
    graphUnderTest.addDirectedEdge("B", "D");
    graphUnderTest.addDirectedEdge("A", "B");
    graphUnderTest.addDirectedEdge("A", "C");
    graphUnderTest.addDirectedEdge("C", "D");
    graphUnderTest.addDirectedEdge("B", "E");
    graphUnderTest.addDirectedEdge("D", "F");
    graphUnderTest.addDirectedEdge("E", "F");

    final var clonedGraphUnderTest = graphUnderTest.clone();
    final var orderedNodesInClone = new ArrayList<>(Collections.singleton("A"));
    orderedNodesInClone.removeIf(clonedGraphUnderTest::nodeExists);
    clonedGraphUnderTest.appendNodesInTopologicalOrder(orderedNodesInClone);
    assertThat(orderedNodesInClone.size(), is(6));
    assertThat(orderedNodesInClone.get(5), is("F"));

    // Clear the original graph.
    graphUnderTest.clearGraph();
    final var orderedNodes = new ArrayList<String>();
    graphUnderTest.appendNodesInTopologicalOrder(orderedNodes);
    assertThat(orderedNodes.size(), is(0));

    // Make sure clone is untouched by the clear on the original.
    orderedNodesInClone.clear();
    clonedGraphUnderTest.appendNodesInTopologicalOrder(orderedNodesInClone);
    assertThat(orderedNodesInClone.size(), is(6));
    assertThat(orderedNodesInClone.get(5), is("F"));
  }

  private static void assertSetEquals(List<String> expectedList, Set<String> actualSet) {
    final var expectedSet = new HashSet<>(expectedList);
    assertEquals(expectedSet, actualSet);
  }

  public static void assertAddEdgeThrowsDagException(
      DirectedAcyclicGraph graphUnderTest, String parent, String child) {
    assertThrows(
        DirectedAcyclicGraphException.class,
        () -> {
          graphUnderTest.addDirectedEdge(parent, child);
        });
  }

  public static void assertDeleteNodeThrowsDagException(
      DirectedAcyclicGraph graphUnderTest, String node) {
    assertThrows(
        DirectedAcyclicGraphException.class,
        () -> {
          graphUnderTest.deleteNodeIfNoChildren(node);
        });
  }
}
