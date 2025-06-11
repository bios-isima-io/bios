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
package io.isima.bios.data.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Set;
import org.junit.Test;

public class DataEngineImplTest {

  @Test
  public void cacheOnlyContextsConfigParser() {
    assertThat(DataEngineImpl.parseCacheOnlyContexts(""), is(Map.of()));
    assertThat(DataEngineImpl.parseCacheOnlyContexts(" "), is(Map.of()));
    assertThat(DataEngineImpl.parseCacheOnlyContexts(" , ,"), is(Map.of()));
    assertThat(DataEngineImpl.parseCacheOnlyContexts(null), is(Map.of()));

    assertThat(
        DataEngineImpl.parseCacheOnlyContexts(
            "pharmeasy.addToCart_ProductIdByObjectId, pharmeasy.otcPdpSignal_productIdbyObjectId"),
        is(
            Map.of(
                "pharmeasy.addtocart_productidbyobjectid",
                Set.of(),
                "pharmeasy.otcpdpsignal_productidbyobjectid",
                Set.of())));

    final var entry1 = "pharmeasy.addToCart_ProductIdByObjectId/ip-10-0-4-201/ip-10-0-4-228";
    final var entry2 = "pharmeasy.otcPdpSignal_productIdbyObjectId/a1.isima.io/a2.isima.io";
    assertThat(
        DataEngineImpl.parseCacheOnlyContexts(entry1 + ", " + entry2),
        is(
            Map.of(
                "pharmeasy.addtocart_productidbyobjectid",
                Set.of("ip-10-0-4-201", "ip-10-0-4-228"),
                "pharmeasy.otcpdpsignal_productidbyobjectid",
                Set.of("a1.isima.io", "a2.isima.io"))));
  }

  @Test
  public void cacheOnlyContextEnabledCheckConventional() {
    final var cacheOnlyContexts =
        DataEngineImpl.parseCacheOnlyContexts(
            "pharmeasy.addToCart_ProductIdByObjectId, pharmeasy.otcPdpSignal_productIdbyObjectId");

    assertTrue(
        DataEngineImpl.isCacheOnlyModeEnabled(
            "pharmeasy", "addToCart_ProductIdByObjectId", "ip-10-0-4-201", cacheOnlyContexts));

    assertTrue(
        DataEngineImpl.isCacheOnlyModeEnabled(
            "pharmeasy", "otcpdpsignal_productIdByObjectId", "ip-10-0-2-108", cacheOnlyContexts));

    assertFalse(
        DataEngineImpl.isCacheOnlyModeEnabled(
            "pharmeasy", "nosuchcontext", "ip-10-0-2-108", cacheOnlyContexts));

    assertFalse(
        DataEngineImpl.isCacheOnlyModeEnabled(
            "nosuchtenant", "addToCart_ProductIdByObjectId", "ip-10-0-4-201", cacheOnlyContexts));

    assertTrue(
        DataEngineImpl.isCacheOnlyModeEnabled(
            "pharmeasy", "otcpdpsignal_productIdByObjectId", "", cacheOnlyContexts));

    assertTrue(
        DataEngineImpl.isCacheOnlyModeEnabled(
            "pharmeasy", "otcpdpsignal_productIdByObjectId", null, cacheOnlyContexts));

    assertFalse(DataEngineImpl.isCacheOnlyModeEnabled("pharmeasy", "bad", "", cacheOnlyContexts));

    assertFalse(
        DataEngineImpl.isCacheOnlyModeEnabled(
            "bad", "otcpdpsignal_productIdByObjectId", null, cacheOnlyContexts));
  }

  @Test
  public void cacheOnlyContextEnabledCheckWithHostSpecs() {
    final var entry1 = "pharmeasy.addToCart_ProductIdByObjectId/ip-10-0-4-201/ip-10-0-4-228";
    final var entry2 = "pharmeasy.otcPdpSignal_productIdbyObjectId/a1.isima.io/a2.isima.io";
    final var cacheOnlyContexts = DataEngineImpl.parseCacheOnlyContexts(entry1 + ", " + entry2);

    assertTrue(
        DataEngineImpl.isCacheOnlyModeEnabled(
            "pharmeasy", "addToCart_ProductIdByObjectId", "ip-10-0-4-201", cacheOnlyContexts));

    assertTrue(
        DataEngineImpl.isCacheOnlyModeEnabled(
            "pharmeasy", "otcpdpsignal_productIdByObjectId", "a2.isima.io", cacheOnlyContexts));

    assertFalse(
        DataEngineImpl.isCacheOnlyModeEnabled(
            "pharmeasy", "addToCart_ProductIdByObjectId", "ip-10-0-6-215", cacheOnlyContexts));

    assertFalse(
        DataEngineImpl.isCacheOnlyModeEnabled(
            "pharmeasy", "nosuchcontext", "ip-10-0-4-201", cacheOnlyContexts));

    assertFalse(
        DataEngineImpl.isCacheOnlyModeEnabled(
            "nosuchtenant", "addToCart_ProductIdByObjectId", "a1.isima.io", cacheOnlyContexts));

    assertFalse(
        DataEngineImpl.isCacheOnlyModeEnabled(
            "pharmeasy", "otcpdpsignal_productIdByObjectId", "ip-10-0-4-228", cacheOnlyContexts));

    assertFalse(
        DataEngineImpl.isCacheOnlyModeEnabled(
            "pharmeasy", "otcpdpsignal_productIdByObjectId", "", cacheOnlyContexts));

    assertTrue(
        DataEngineImpl.isCacheOnlyModeEnabled(
            "pharmeasy", "otcpdpsignal_productIdByObjectId", null, cacheOnlyContexts));

    assertFalse(DataEngineImpl.isCacheOnlyModeEnabled("pharmeasy", "bad", null, cacheOnlyContexts));

    assertFalse(DataEngineImpl.isCacheOnlyModeEnabled("pharmeasy", "bad", "", cacheOnlyContexts));

    assertFalse(
        DataEngineImpl.isCacheOnlyModeEnabled(
            "bad", "otcpdpsignal_productIdByObjectId", null, cacheOnlyContexts));
  }
}
