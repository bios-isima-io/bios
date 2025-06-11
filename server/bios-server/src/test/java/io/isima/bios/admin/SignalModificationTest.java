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
package io.isima.bios.admin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import io.isima.bios.errors.exception.AdminChangeRequestToSameException;
import io.isima.bios.metrics.MetricsStreamProvider;
import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.AttributeValueGeneric;
import io.isima.bios.models.MissingAttributePolicy;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.TenantConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SignalModificationTest {

  private Admin admin;
  private io.isima.bios.admin.v1.impl.AdminImpl tfosAdmin;

  private static String tenantName;
  // private TenantConfig tenantConfig;
  private SignalConfig simpleSignal;

  @BeforeClass
  public static void setUpClass() {
    tenantName = "signalModificationTest";
  }

  @Before
  public void setUp() throws Exception {
    tfosAdmin = new io.isima.bios.admin.v1.impl.AdminImpl(null, new MetricsStreamProvider());
    admin = new AdminImpl(tfosAdmin, null, null);

    final var tenantConfig = new TenantConfig("signalModificationTest");
    Long timestamp = System.currentTimeMillis();
    admin.createTenant(tenantConfig, RequestPhase.INITIAL, timestamp, false);
    admin.createTenant(tenantConfig, RequestPhase.FINAL, timestamp, false);

    // Make a simple signal that can be used as a template
    simpleSignal = new SignalConfig();
    simpleSignal.setName("simpleSignal");
    simpleSignal.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
    final var attributes = new ArrayList<AttributeConfig>();
    attributes.add(new AttributeConfig("first", AttributeType.STRING));
    simpleSignal.setAttributes(attributes);
  }

  @Test
  public void modifyStringEnum() throws Exception {

    final var allowedValuesOrig = new ArrayList<AttributeValueGeneric>();
    allowedValuesOrig.add(new AttributeValueGeneric("ONE", AttributeType.STRING));
    allowedValuesOrig.add(new AttributeValueGeneric("TWO", AttributeType.STRING));
    allowedValuesOrig.add(new AttributeValueGeneric("THREE", AttributeType.STRING));

    simpleSignal.getAttributes().get(0).setAllowedValues(allowedValuesOrig);

    Long timestamp = System.currentTimeMillis();
    admin.createSignal(tenantName, simpleSignal, RequestPhase.INITIAL, timestamp);
    final var initialSignal =
        admin.createSignal(tenantName, simpleSignal, RequestPhase.FINAL, timestamp);
    assertEquals(timestamp, initialSignal.getVersion());
    assertEquals(timestamp, initialSignal.getBiosVersion());
    assertEquals(
        List.of("ONE", "TWO", "THREE"),
        initialSignal.getAttributes().get(0).getAllowedValues().stream()
            .map((value) -> value.asString())
            .collect(Collectors.toList()));

    final var allowedValuesModified = new ArrayList<>(allowedValuesOrig);
    allowedValuesModified.add(new AttributeValueGeneric("FOUR", AttributeType.STRING));
    simpleSignal.getAttributes().get(0).setAllowedValues(allowedValuesModified);
    timestamp += 1;
    final var signalName = simpleSignal.getName();
    admin.updateSignal(tenantName, signalName, simpleSignal, RequestPhase.INITIAL, timestamp);
    admin.updateSignal(tenantName, signalName, simpleSignal, RequestPhase.FINAL, timestamp);
    final var modifiedSignal =
        admin.getSignals(tenantName, true, false, false, Set.of(signalName)).get(0);
    assertEquals(timestamp, modifiedSignal.getVersion());
    assertEquals(initialSignal.getBiosVersion(), modifiedSignal.getBiosVersion());
    assertEquals(
        List.of("ONE", "TWO", "THREE", "FOUR"),
        modifiedSignal.getAttributes().get(0).getAllowedValues().stream()
            .map((value) -> value.asString())
            .collect(Collectors.toList()));
  }

  @Test
  public void modifyStringIdenticalEnum() throws Exception {

    final var allowedValuesOrig = new ArrayList<AttributeValueGeneric>();
    allowedValuesOrig.add(new AttributeValueGeneric("ONE", AttributeType.STRING));
    allowedValuesOrig.add(new AttributeValueGeneric("TWO", AttributeType.STRING));
    allowedValuesOrig.add(new AttributeValueGeneric("THREE", AttributeType.STRING));

    simpleSignal.getAttributes().get(0).setAllowedValues(allowedValuesOrig);

    Long timestamp = System.currentTimeMillis();
    admin.createSignal(tenantName, simpleSignal, RequestPhase.INITIAL, timestamp);
    admin.createSignal(tenantName, simpleSignal, RequestPhase.FINAL, timestamp);

    timestamp += 1;
    final var signalName = simpleSignal.getName();
    try {
      admin.updateSignal(tenantName, signalName, simpleSignal, RequestPhase.INITIAL, timestamp);
      fail("Exception is expected");
    } catch (AdminChangeRequestToSameException e) {
      //
    }
  }
}
