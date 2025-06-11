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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.metrics.MetricsStreamProvider;
import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.DataSketchType;
import io.isima.bios.models.PostStorageStageConfig;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.SignalFeatureConfig;
import io.isima.bios.models.TenantConfig;
import java.util.List;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class DataSketchesTest {
  private Admin admin;
  private static String tenantName;
  private static Long timestamp;

  @BeforeClass
  public static void setUpClass() {
    tenantName = "dataSketchesTest";
    timestamp = System.currentTimeMillis();
  }

  @Before
  public void setUp() throws Exception {
    io.isima.bios.admin.v1.impl.AdminImpl tfosAdmin =
        new io.isima.bios.admin.v1.impl.AdminImpl(null, new MetricsStreamProvider());
    admin = new AdminImpl(tfosAdmin, null, null);

    final var tenantConfig = new TenantConfig(tenantName);
    admin.createTenant(tenantConfig, RequestPhase.INITIAL, ++timestamp, false);
    admin.createTenant(tenantConfig, RequestPhase.FINAL, timestamp, false);
  }

  @Test
  public void dataSketchesRepeatedEntries() throws Exception {
    var signalConfig =
        TestUtils.getSignalConfig("src/test/resources/data-sketches-signal-bios-violation1.json");
    createSignalExpectError(signalConfig);
  }

  @Test
  public void dataSketchesRepeatedEntries2() throws Exception {
    var signal = new SignalConfig("dataSketches");
    signal.setAttributes(List.of(new AttributeConfig("orderId", AttributeType.INTEGER)));
    signal.setPostStorageStage(new PostStorageStageConfig());
    var feature = new SignalFeatureConfig();
    feature.setName("feature1");
    feature.setDataSketches(List.of(DataSketchType.MOMENTS, DataSketchType.MOMENTS));
    signal.getPostStorageStage().setFeatures(List.of(feature));

    createSignalExpectError(signal);
  }

  @Test
  @Ignore("BIOS-6029")
  public void dataSketchesAndAttributeTypes() throws Exception {
    var signal =
        TestUtils.getSignalConfig("src/test/resources/data-sketches-signal-bios-violation2.json");
    signal.setPostStorageStage(new PostStorageStageConfig());
    var feature = new SignalFeatureConfig();
    feature.setName("feature1");
    feature.setFeatureInterval(300000L);
    signal.getPostStorageStage().setFeatures(List.of(feature));

    feature.setAttributes(List.of("attrString"));
    testSketchCombinations(signal, feature);

    feature.setAttributes(List.of("attrInteger", "attrString"));
    testSketchCombinations(signal, feature);

    feature.setAttributes(List.of("attrDecimal", "attrString"));
    testSketchCombinations(signal, feature);

    feature.setAttributes(List.of("attrString", "attrDecimal", "attrInteger"));
    testSketchCombinations(signal, feature);
  }

  private void testSketchCombinations(SignalConfig signal, SignalFeatureConfig feature)
      throws TfosException, ApplicationException {
    feature.setDataSketches(List.of());
    createSignalExpectSuccess(signal);

    feature.setDataSketches(List.of(DataSketchType.MOMENTS));
    createSignalExpectError(signal);

    feature.setDataSketches(List.of(DataSketchType.QUANTILES));
    createSignalExpectError(signal);

    feature.setDataSketches(List.of(DataSketchType.QUANTILES, DataSketchType.MOMENTS));
    createSignalExpectError(signal);

    feature.setDataSketches(
        List.of(DataSketchType.QUANTILES, DataSketchType.MOMENTS, DataSketchType.DISTINCT_COUNT));
    createSignalExpectError(signal);
  }

  private void createSignalExpectError(SignalConfig signal)
      throws TfosException, ApplicationException {
    boolean exceptionCaught = false;
    try {
      admin.createSignal(tenantName, signal, RequestPhase.INITIAL, ++timestamp);
      admin.createSignal(tenantName, signal, RequestPhase.FINAL, timestamp);
    } catch (ConstraintViolationException e) {
      exceptionCaught = true;
    }
    assertTrue(exceptionCaught);
  }

  private void createSignalExpectSuccess(SignalConfig signal)
      throws TfosException, ApplicationException {
    boolean exceptionCaught = false;
    try {
      admin.deleteSignal(tenantName, signal.getName(), RequestPhase.INITIAL, ++timestamp);
      admin.deleteSignal(tenantName, signal.getName(), RequestPhase.FINAL, timestamp);
    } catch (TfosException e) {
      // ignore
    }
    try {
      admin.createSignal(tenantName, signal, RequestPhase.INITIAL, ++timestamp);
      admin.createSignal(tenantName, signal, RequestPhase.FINAL, timestamp);
    } catch (ConstraintViolationException e) {
      exceptionCaught = true;
      System.out.println(e.toString());
    } finally {
      try {
        admin.deleteSignal(tenantName, signal.getName(), RequestPhase.INITIAL, ++timestamp);
        admin.deleteSignal(tenantName, signal.getName(), RequestPhase.FINAL, timestamp);
      } catch (TfosException e) {
        // just ignore
      }
    }
    assertFalse(exceptionCaught);
  }
}
