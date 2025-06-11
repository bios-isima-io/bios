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

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;

public class StreamOrderTest {

  // A --> B
  // C --> B
  // C -----> D
  // E -----> D ------> F
  // G ----------> H -> F -> I -> J
  // W -> X -> Y
  // Z, U, V have no dependencies; intersperse them in between the above list of
  // nodes.
  @Test
  public void testDependencyOrderForStreamModification() throws Throwable {
    TenantDesc tenant = new TenantDesc("testTenant", 1L, false);

    final var Z = new StreamDesc("Z", 1L);
    tenant.addStream(Z);
    final var A = new StreamDesc("A", 1L);
    tenant.addStream(A);
    final var B = new StreamDesc("B", 1L);
    tenant.addStream(B);
    final var C = new StreamDesc("C", 1L);
    tenant.addStream(C);
    final var U = new StreamDesc("U", 1L);
    tenant.addStream(U);
    final var D = new StreamDesc("D", 1L);
    tenant.addStream(D);
    final var E = new StreamDesc("E", 1L);
    tenant.addStream(E);
    final var F = new StreamDesc("F", 1L);
    tenant.addStream(F);
    final var V = new StreamDesc("V", 1L);
    tenant.addStream(V);
    final var G = new StreamDesc("G", 1L);
    tenant.addStream(G);
    final var J = new StreamDesc("J", 1L);
    tenant.addStream(J);
    final var I = new StreamDesc("I", 1L);
    tenant.addStream(I);
    final var H = new StreamDesc("H", 1L);
    tenant.addStream(H);
    final var Y = new StreamDesc("Y", 1L);
    tenant.addStream(Y);
    final var W = new StreamDesc("W", 1L);
    tenant.addStream(W);
    final var X = new StreamDesc("X", 1L);
    tenant.addStream(X);
    Set<String> allNodes =
        new HashSet<>(
            Arrays.asList(
                "Z", "A", "B", "C", "U", "D", "E", "F", "V", "G", "J", "I", "H", "Y", "W", "X"));
    assertEquals(
        allNodes,
        tenant.getAllStreams().stream()
            .map((s) -> s.getName())
            .collect(Collectors.toUnmodifiableSet()));

    tenant.addDependency(A, B);
    tenant.addDependency(C, B);
    tenant.addDependency(C, D);
    tenant.addDependency(E, D);
    tenant.addDependency(D, F);
    tenant.addDependency(H, F);
    tenant.addDependency(G, H);
    tenant.addDependency(F, I);
    tenant.addDependency(I, J);
    tenant.addDependency(W, X);
    tenant.addDependency(X, Y);

    verifyDependencyAfter(tenant, "A", Arrays.asList("B"));
    verifyDependencyAfter(tenant, "B", Arrays.asList());
    verifyDependencyAfter(tenant, "C", Arrays.asList("B", "D", "F", "I", "J"));
    verifyDependencyAfter(tenant, "D", Arrays.asList("F", "I", "J"));
    verifyDependencyAfter(tenant, "E", Arrays.asList("D", "F", "I", "J"));
    verifyDependencyAfter(tenant, "F", Arrays.asList("I", "J"));
    verifyDependencyAfter(tenant, "G", Arrays.asList("H", "F", "I", "J"));
    verifyDependencyAfter(tenant, "H", Arrays.asList("F", "I", "J"));
    verifyDependencyAfter(tenant, "I", Arrays.asList("J"));
    verifyDependencyAfter(tenant, "J", Arrays.asList());
    verifyDependencyAfter(tenant, "X", Arrays.asList("Y"));
    verifyDependencyAfter(tenant, "Y", Arrays.asList());
    verifyDependencyAfter(tenant, "Z", Arrays.asList());
    verifyDependencyAfter(tenant, "U", Arrays.asList());
    verifyDependencyAfter(tenant, "V", Arrays.asList());
    verifyDependencyAfter(tenant, "W", Arrays.asList("X", "Y"));
  }

  public static void verifyDependencyAfter(TenantDesc tenant, String node, List<String> after) {
    final var streamDesc = tenant.getStream(node);
    final var list = tenant.getAllStreamsInDependencyOrder(streamDesc, streamDesc);
    // System.out.print(node);
    // System.out.println(list.stream().map((s) -> s.getName())
    //     .collect(Collectors.toUnmodifiableList()));
    // Only the items listed as after should appear after the given node.
    for (final var afterItem : after) {
      assertThat(
          afterItem,
          list.indexOf(tenant.getStream(afterItem)),
          greaterThan(list.indexOf(tenant.getStream(node))));
    }
    // Everything else should appear before the node.
    Set<String> remainingNodes =
        tenant.getAllStreams().stream().map((s) -> s.getName()).collect(Collectors.toSet());
    remainingNodes.removeAll(after);
    remainingNodes.remove(node);
    for (final var beforeItem : remainingNodes) {
      assertThat(
          beforeItem,
          list.indexOf(tenant.getStream(beforeItem)),
          lessThan(list.indexOf(tenant.getStream(node))));
    }
  }

  @Test
  public void testVersionOrder() {
    long version = 0L;
    TenantDesc tenant = new TenantDesc("testTenant", ++version, false);

    tenant.addStream(new StreamDesc("A", ++version));
    tenant.addStream(new StreamDesc("B", ++version));
    verifyVersionOrder(tenant);
    tenant.addStream(new StreamDesc("C", ++version));
    tenant.addStream(new StreamDesc("D", ++version));
    verifyVersionOrder(tenant);
    tenant.addStream(new StreamDesc("B", ++version).setDeleted(true));
    verifyVersionOrder(tenant);
    tenant.addStream(new StreamDesc("A", ++version));
    tenant.addStream(new StreamDesc("D", ++version));
    tenant.addStream(new StreamDesc("B", ++version));
    verifyVersionOrder(tenant);
    tenant.addStream(new StreamDesc("D", ++version).setDeleted(true));
    verifyVersionOrder(tenant);
    tenant.addStream(new StreamDesc("B", ++version).setDeleted(true));
    verifyVersionOrder(tenant);
    tenant.addStream(new StreamDesc("D", ++version));
    verifyVersionOrder(tenant);
    tenant.addStream(new StreamDesc("C", ++version));
    tenant.addStream(new StreamDesc("D", ++version));
    verifyVersionOrder(tenant);
    tenant.addStream(new StreamDesc("C", ++version));
    tenant.addStream(new StreamDesc("D", ++version));
    verifyVersionOrder(tenant);
  }

  public static void verifyVersionOrder(TenantDesc tenant) {
    final var list = tenant.getNonDeletedStreamVersionsInVersionOrder();
    // list.forEach(s -> System.out.printf("%s%d, ", s.getName(), s.getVersion()));
    // System.out.println();

    // Verify that the versions are in strictly increasing order and streams are not deleted.
    long currentVersion = 0;
    for (int i = 0; i < list.size(); i++) {
      final var s = list.get(i);
      assertThat(s.getName(), currentVersion, lessThan(s.getVersion()));
      currentVersion = s.getVersion();
      assertFalse(s.getName(), s.isDeleted());
    }
  }
}
