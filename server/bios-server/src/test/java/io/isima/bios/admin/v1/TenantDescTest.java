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
import static org.junit.Assert.fail;

import io.isima.bios.admin.v1.impl.AdminImpl;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.metrics.MetricsConstants;
import io.isima.bios.metrics.MetricsStreamProvider;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.StreamType;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TenantDescTest {
  private static Validator validator;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    validator = factory.getValidator();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testOneStream() {
    Long timestamp = System.currentTimeMillis();
    final TenantDesc tenantDesc = new TenantDesc("oneStream", timestamp, false);
    final String streamName = "ElasticFlash";
    // Add the first stream
    tenantDesc.addStream(new StreamDesc(streamName, timestamp, false));

    verifySimpleGetStream(tenantDesc, true, streamName, streamName, timestamp);
    verifySimpleGetStream(tenantDesc, true, streamName.toUpperCase(), streamName, timestamp);
    verifySimpleGetStream(tenantDesc, true, streamName.toLowerCase(), streamName, timestamp);
    verifyGetLatestStream(tenantDesc, true, streamName, false, streamName, timestamp, false);
    verifyGetLatestStream(
        tenantDesc, true, streamName.toLowerCase(), false, streamName, timestamp, false);
    verifyGetLatestStream(
        tenantDesc, true, streamName.toUpperCase(), false, streamName, timestamp, false);
    verifyGetLatestStream(tenantDesc, true, streamName, true, streamName, timestamp, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp, true, streamName, false);
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp + 1, true, null, false);

    // Insert an older stream
    tenantDesc.addStream(new StreamDesc(streamName, timestamp - 1, false));

    verifySimpleGetStream(tenantDesc, true, streamName, streamName, timestamp);
    verifyGetLatestStream(tenantDesc, true, streamName, false, streamName, timestamp, false);
    verifyGetLatestStream(tenantDesc, true, streamName, true, streamName, timestamp, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp, true, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp, true, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp - 1, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp - 1, true, streamName, false);
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp + 11, true, null, false);

    // Add a newer stream
    final String streamName2 = streamName.toUpperCase();
    tenantDesc.addStream(new StreamDesc(streamName2, timestamp + 10, false));

    verifySimpleGetStream(tenantDesc, true, streamName, streamName2, timestamp + 10);
    verifySimpleGetStream(tenantDesc, true, streamName.toUpperCase(), streamName2, timestamp + 10);
    verifySimpleGetStream(tenantDesc, true, streamName.toLowerCase(), streamName2, timestamp + 10);
    verifyGetLatestStream(tenantDesc, true, streamName, false, streamName2, timestamp + 10, false);
    verifyGetLatestStream(
        tenantDesc, true, streamName.toLowerCase(), false, streamName2, timestamp + 10, false);
    verifyGetLatestStream(
        tenantDesc, true, streamName.toUpperCase(), false, streamName2, timestamp + 10, false);
    verifyGetLatestStream(tenantDesc, true, streamName, true, streamName2, timestamp + 10, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 10, false, streamName2, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 10, true, streamName2, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp, true, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp - 1, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp - 1, true, streamName, false);
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp + 11, true, null, false);

    // Try deleting the stream but the version is not the latest
    tenantDesc.addStream(new StreamDesc(streamName, timestamp + 9, true));

    verifySimpleGetStream(tenantDesc, true, streamName, streamName2, timestamp + 10);
    verifyGetLatestStream(tenantDesc, true, streamName, false, streamName2, timestamp + 10, false);
    verifyGetLatestStream(tenantDesc, true, streamName, true, streamName2, timestamp + 10, false);

    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 10, false, streamName2, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 10, true, streamName2, false);
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp + 9, false, null, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 9, true, streamName, true);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp, true, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp - 1, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp - 1, true, streamName, false);

    // Delete with the latest version
    tenantDesc.addStream(new StreamDesc(streamName, timestamp + 11, true));

    verifySimpleGetStream(tenantDesc, false, streamName, null, 0L);
    verifyGetLatestStream(tenantDesc, false, streamName, false, null, 0L, false);
    verifyGetLatestStream(tenantDesc, true, streamName, true, streamName, timestamp + 11, true);

    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp + 11, false, null, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 11, true, streamName, true);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 10, false, streamName2, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 10, true, streamName2, false);
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp + 9, false, null, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 9, true, streamName, true);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp, true, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp - 1, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp - 1, true, streamName, false);

    // Recreate a newer stream
    tenantDesc.addStream(new StreamDesc(streamName, timestamp + 12, false));

    verifySimpleGetStream(tenantDesc, true, streamName, streamName, timestamp + 12);
    verifyGetLatestStream(tenantDesc, true, streamName, false, streamName, timestamp + 12, false);

    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 12, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp + 11, false, null, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 11, true, streamName, true);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 10, false, streamName2, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 10, true, streamName2, false);
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp + 9, false, null, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 9, true, streamName, true);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp, true, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp - 1, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp - 1, true, streamName, false);

    // Test getting a non-existing stream
    assertNull(tenantDesc.getStream("nonExisting"));
    assertNull(tenantDesc.getStream("nonExisting", false));
    assertNull(tenantDesc.getStream("nonExisting", true));
    assertNull(tenantDesc.getStream("nonExisting", timestamp, true));

    // prepare for stream trimming test
    tenantDesc.addStream(new StreamDesc("one", timestamp, false));
    tenantDesc.addStream(new StreamDesc("two", timestamp + 2, false));
    tenantDesc.addStream(new StreamDesc("three", timestamp + 4, false));
    tenantDesc.addStream(new StreamDesc("four", timestamp + 8, false));
    tenantDesc.addStream(new StreamDesc("five", timestamp + 16, false));
    tenantDesc.addStream(new StreamDesc("six", timestamp + 32, false));

    verifyGetsWithVersion(tenantDesc, true, "one", timestamp, false, "one", false);
    verifyGetsWithVersion(tenantDesc, true, "two", timestamp + 2, false, "two", false);
    verifyGetsWithVersion(tenantDesc, true, "three", timestamp + 4, false, "three", false);
    verifyGetsWithVersion(tenantDesc, true, "four", timestamp + 8, false, "four", false);
    verifyGetsWithVersion(tenantDesc, true, "five", timestamp + 16, false, "five", false);
    verifyGetsWithVersion(tenantDesc, true, "six", timestamp + 32, false, "six", false);

    // trim streams
    tenantDesc.trimOldStreams(timestamp);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 12, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp + 11, false, null, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 11, true, streamName, true);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 10, false, streamName2, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 10, true, streamName2, false);
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp + 9, false, null, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 9, true, streamName, true);
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp, true, null, false);
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp - 1, true, null, false);

    // trim at advanced timestamp
    tenantDesc.trimOldStreams(timestamp + 16);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 12, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp + 11, false, null, false);
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp + 11, true, null, true);
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp + 10, false, null, false);
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp + 10, true, null, false);
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp + 9, false, null, false);
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp + 9, true, null, true);
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp, true, null, false);

    // delete the stream
    tenantDesc.addStream(new StreamDesc(streamName, timestamp + 20, true));
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp + 20, false, null, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 20, true, streamName, true);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 12, false, streamName, false);

    // cleanup again
    tenantDesc.trimOldStreams(timestamp + 20);
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp + 20, false, null, false);
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp + 20, true, null, true);
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp + 12, false, null, false);

    // verify other non-deleted streams get no effects
    verifyGetsWithVersion(tenantDesc, true, "one", timestamp, false, "one", false);
    verifyGetsWithVersion(tenantDesc, true, "two", timestamp + 2, false, "two", false);
    verifyGetsWithVersion(tenantDesc, true, "three", timestamp + 4, false, "three", false);
    verifyGetsWithVersion(tenantDesc, true, "four", timestamp + 8, false, "four", false);
    verifyGetsWithVersion(tenantDesc, true, "five", timestamp + 16, false, "five", false);
    verifyGetsWithVersion(tenantDesc, true, "six", timestamp + 32, false, "six", false);
  }

  @Test
  public void testStreamCleanup() {
    Long timestamp = System.currentTimeMillis();
    final TenantDesc tenantDesc = new TenantDesc("streamCleanup", timestamp, false);
    final String streamName = "TiredFractals";

    // add streams that are not linked
    tenantDesc.addStream(new StreamDesc(streamName, timestamp, false));

    // make the first line of stream mod histroy
    final StreamDesc first1 = new StreamDesc(streamName, timestamp + 1, false);
    first1.addAttribute(new AttributeDesc("one", InternalAttributeType.STRING));
    final StreamDesc first2 = new StreamDesc(streamName, timestamp + 4, false);
    first2.addAttribute(new AttributeDesc("two", InternalAttributeType.STRING));
    first2.setPrevName(first1.getName());
    first2.setPrevVersion(first1.getVersion());
    final StreamDesc first3 = new StreamDesc(streamName, timestamp + 6, false);
    first3.addAttribute(new AttributeDesc("three", InternalAttributeType.STRING));
    first3.setPrevName(first2.getName());
    first3.setPrevVersion(first2.getVersion());

    tenantDesc.addStream(first1);
    tenantDesc.addStream(first2);
    tenantDesc.addStream(first3);

    // make the second line of stream mod history
    final StreamDesc second1 = new StreamDesc(streamName, timestamp + 2, false);
    second1.addAttribute(new AttributeDesc("ek", InternalAttributeType.STRING));
    final StreamDesc second2 = new StreamDesc(streamName, timestamp + 5, false);
    second2.addAttribute(new AttributeDesc("do", InternalAttributeType.STRING));
    second2.setPrevName(second1.getName());
    second2.setPrevVersion(second1.getVersion());

    tenantDesc.addStream(second1);
    tenantDesc.addStream(second2);

    // add streams that are not linked
    tenantDesc.addStream(new StreamDesc(streamName, timestamp, false));
    tenantDesc.addStream(new StreamDesc(streamName, timestamp + 3, false));

    // verify streams before the cleanup
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 1, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 2, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 3, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 4, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 5, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 6, false, streamName, false);

    // try cleanup but the time has not come yet.
    tenantDesc.trimOldStreams(timestamp - 1);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 1, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 2, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 3, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 4, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 5, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 6, false, streamName, false);

    // cleanup until the middle, links to the latest should remain
    tenantDesc.trimOldStreams(timestamp + 2);
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp, true, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 1, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp + 2, true, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 3, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 4, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 5, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 6, false, streamName, false);

    // full clean up
    tenantDesc.trimOldStreams(timestamp + 20);
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp, true, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 1, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp + 2, true, streamName, false);
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp + 3, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 4, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp + 5, false, streamName, false);
    verifyGetsWithVersion(tenantDesc, true, streamName, timestamp + 6, false, streamName, false);

    // verify that internals of the streams are preserved properly.
    final StreamDesc streamOne = tenantDesc.getStream(streamName, timestamp + 1, false);
    assertEquals("one", streamOne.getAttributes().get(0).getName());
    final StreamDesc streamTwo = tenantDesc.getStream(streamName, timestamp + 4, false);
    assertEquals("two", streamTwo.getAttributes().get(0).getName());
    final StreamDesc streamThree = tenantDesc.getStream(streamName, timestamp + 6, false);
    assertEquals("three", streamThree.getAttributes().get(0).getName());

    // delete the stream and cleanup
    tenantDesc.addStream(new StreamDesc(streamName, timestamp + 21, true));
    tenantDesc.trimOldStreams(timestamp + 25);
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp + 1, true, streamName, false);
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp + 4, true, streamName, false);
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp + 6, true, streamName, false);
    verifyGetsWithVersion(tenantDesc, false, streamName, timestamp + 21, true, streamName, false);
  }

  @Test
  public void testMultiStreams() throws NoSuchStreamException {
    Long timestamp = System.currentTimeMillis();
    final TenantDesc tenantDesc = new TenantDesc("multiStreams", timestamp, false);

    final StreamDesc ichi = new StreamDesc("Ichi", timestamp, false);
    tenantDesc.addStream(ichi);

    final StreamDesc deleted = new StreamDesc("deleted", ++timestamp, true);
    tenantDesc.addStream(deleted);

    final StreamDesc san = new StreamDesc("san", ++timestamp, false);
    tenantDesc.addStream(san);

    final StreamDesc sanIndex = new StreamDesc("san.index.abc", ++timestamp, false);
    sanIndex.setType(StreamType.INDEX);
    tenantDesc.addStream(sanIndex);

    final StreamDesc sanView = new StreamDesc("san.view.abc", ++timestamp, false);
    sanView.setType(StreamType.VIEW);
    tenantDesc.addStream(sanView);

    final StreamDesc sanRollup = new StreamDesc("san_rollup_abc", ++timestamp, false);
    sanRollup.setType(StreamType.ROLLUP);
    tenantDesc.addStream(sanRollup);

    final StreamDesc internalSignal = new StreamDesc("_internal_signal", ++timestamp, false);
    tenantDesc.addStream(internalSignal);

    final StreamDesc internalContext = new StreamDesc("_internal_context", ++timestamp, false);
    internalContext.setType(StreamType.CONTEXT);
    tenantDesc.addStream(internalContext);

    final StreamDesc ni = new StreamDesc("Ni", ++timestamp, false);
    ni.setType(StreamType.CONTEXT);
    tenantDesc.addStream(ni);

    final StreamDesc metrics = new StreamDesc("_metrics", ++timestamp, false);
    metrics.setType(StreamType.METRICS);
    tenantDesc.addStream(metrics);

    final StreamDesc invalidMetrics = new StreamDesc("invalid_metrics", ++timestamp, false);
    invalidMetrics.setType(StreamType.METRICS);
    tenantDesc.addStream(invalidMetrics);

    final List<StreamDesc> simple = tenantDesc.getStreams();
    assertEquals(7, simple.size());
    contains(simple, "Ichi", StreamType.SIGNAL);
    contains(simple, "Ni", StreamType.CONTEXT);
    contains(simple, "san", StreamType.SIGNAL);
    contains(simple, "san.index.abc", StreamType.INDEX);
    contains(simple, "san.view.abc", StreamType.VIEW);
    contains(simple, "san_rollup_abc", StreamType.ROLLUP);
    contains(simple, "invalid_metrics", StreamType.METRICS);

    final List<StreamDesc> all = tenantDesc.getAllStreams();
    assertEquals(10, all.size());
    contains(all, "Ichi", StreamType.SIGNAL);
    contains(all, "Ni", StreamType.CONTEXT);
    contains(all, "san", StreamType.SIGNAL);
    contains(all, "san.index.abc", StreamType.INDEX);
    contains(all, "san.view.abc", StreamType.VIEW);
    contains(all, "san_rollup_abc", StreamType.ROLLUP);
    contains(all, "invalid_metrics", StreamType.METRICS);
    contains(all, "_internal_signal", StreamType.SIGNAL);
    contains(all, "_internal_context", StreamType.CONTEXT);
    contains(all, "_metrics", StreamType.METRICS);

    final StreamDesc metrics2 = new StreamDesc("_metrics", ++timestamp, true);
    metrics2.setType(StreamType.METRICS);
    tenantDesc.addStream(metrics2);

    final StreamDesc invalidMetrics2 = new StreamDesc("invalid_metrics", ++timestamp, true);
    invalidMetrics2.setType(StreamType.METRICS);
    tenantDesc.addStream(invalidMetrics2);
  }

  private void contains(Collection<StreamDesc> streams, String name, StreamType type) {
    for (StreamDesc stream : streams) {
      if (stream.getName().equals(name)) {
        assertEquals(type, stream.getType());
        return;
      }
    }
    fail(String.format("Stream %s is not in the streams %s", name, streams));
  }

  @Test
  public void testGeneratingOperationalMetricsStream() throws Exception {
    final Long timestamp = System.currentTimeMillis();
    final AdminImpl admin = new AdminImpl(null, new MetricsStreamProvider());
    final TenantDesc tenantDesc = new TenantDesc("metrics_test", timestamp, false);
    final StreamDesc metricsStream = admin.generateOperationalMetricsStream(tenantDesc);
    metricsStream.setVersion(timestamp);
    final Set<ConstraintViolation<StreamDesc>> constraintViolations =
        validator.validate(metricsStream);
    assertFalse(constraintViolations.toString(), constraintViolations.isEmpty());

    System.out.println(metricsStream);

    assertEquals(MetricsConstants.BIOS_TENANT_OP_METRICS_SIGNAL, metricsStream.getName());
    assertEquals(StreamType.SIGNAL, metricsStream.getType());
    assertEquals(timestamp, metricsStream.getVersion());
  }

  @Test
  public void testSettingMetricsStream() throws Exception {
    final Long timestamp = System.currentTimeMillis();
    final TenantDesc tenantDesc = new TenantDesc("test_settings_metrics", timestamp, false);
    final AdminImpl admin = new AdminImpl(null, new MetricsStreamProvider());

    // Set metrics stream
    admin.setMetricsStream(tenantDesc);

    final StreamDesc operationalMetricsStream =
        tenantDesc.getStream(MetricsConstants.BIOS_TENANT_OP_METRICS_SIGNAL);
    assertNotNull(operationalMetricsStream);

    final StreamDesc operationalBackup = operationalMetricsStream.duplicate();

    // Set again. Metrics stream should not change this time.
    admin.setMetricsStream(tenantDesc);

    // make sure the original has not changed.
    assertEquals(operationalBackup, operationalMetricsStream);

    // retrieve the metrics stream again and make sure nothing has changed.
    final StreamDesc operationalMetricsStream2 =
        tenantDesc.getStream(MetricsConstants.BIOS_TENANT_OP_METRICS_SIGNAL);
    assertEquals(operationalMetricsStream, operationalMetricsStream2);
  }

  @Test
  public void testMakeAlias() {
    TenantDesc tenantDesc = new TenantDesc("name", System.currentTimeMillis(), false);
    assertEquals("hello", tenantDesc.makeAlias("hello"));
    assertEquals("hello_world", tenantDesc.makeAlias("hello_world"));
    assertEquals("hello", tenantDesc.makeAlias("hello_signal"));
    assertEquals("hello_world", tenantDesc.makeAlias("hello_world_signal"));
    assertEquals("hello", tenantDesc.makeAlias("hello_signal_123"));
    assertEquals("appearing", tenantDesc.makeAlias("appearing_signal_twice_signal_123"));
    assertEquals(
        "do_we_want_to_trim_word", tenantDesc.makeAlias("do_we_want_to_trim_word_signaling"));
  }

  @Test
  public void testResolveAliases() throws NoSuchStreamException {
    long timestamp = System.currentTimeMillis();
    TenantDesc tenantDesc = new TenantDesc("resolve", timestamp, false);
    tenantDesc.addStream(new StreamDesc("one", ++timestamp, false));
    tenantDesc.addStream(new StreamDesc("two_signal", ++timestamp, false));
    tenantDesc.addStream(new StreamDesc("visitor_record_signal_1", ++timestamp, false));
    tenantDesc.addStream(new StreamDesc("transaction_event_signal2", ++timestamp, false));
    tenantDesc.buildSignalAliases();
    assertEquals("one", tenantDesc.resolveSignalName("one"));
    assertEquals("two_signal", tenantDesc.resolveSignalName("two"));
    assertEquals("visitor_record_signal_1", tenantDesc.resolveSignalName("visitor_record"));
    assertEquals("transaction_event_signal2", tenantDesc.resolveSignalName("transaction_event"));
    try {
      tenantDesc.resolveSignalName("not_existing");
      fail("exception is expected");
    } catch (TfosException e) {
      // expected
    }
    try {
      tenantDesc.resolveSignalName(null);
      fail("exception is expected");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testStreamNameProxy() {
    long timestamp = System.currentTimeMillis();
    TenantDesc tenantDesc = new TenantDesc("proxy", timestamp, false);
    var s1 = new StreamDesc("one", ++timestamp, false);
    var s2 = new StreamDesc("two", ++timestamp, false);
    tenantDesc.addStream(s1);
    tenantDesc.addStream(s2);
    assertNull(tenantDesc.getMaxAllocatedStreamNameProxy());
    tenantDesc.assignStreamNameProxy(s1);
    assertEquals(1, s1.getStreamNameProxy().intValue());
    assertEquals(1, tenantDesc.getMaxAllocatedStreamNameProxy().intValue());
    tenantDesc.assignStreamNameProxy(s2);
    assertEquals(1, s1.getStreamNameProxy().intValue());
    assertEquals(2, s2.getStreamNameProxy().intValue());
    assertEquals(2, tenantDesc.getMaxAllocatedStreamNameProxy().intValue());
    System.out.println(tenantDesc.toString());
    System.out.println(s2.toString());
  }

  /**
   * Subroutine to verify method getStream(name) behavior.
   *
   * @param tenantDesc Target tenant descriptor.
   * @param expectReturn Whether the tried getStream() would return a non-null result.
   * @param key The key to specify the stream.
   * @param streamName Expected stream name. Not used when expectedReturn is false.
   * @param version Expected version. Not used when expectedReturns is false.
   */
  private void verifySimpleGetStream(
      TenantDesc tenantDesc, boolean expectReturn, String key, String streamName, Long version) {
    final StreamDesc stream = tenantDesc.getStream(key);
    if (expectReturn) {
      assertNotNull(stream);
      assertEquals(streamName, stream.getName());
      assertEquals(version, stream.getVersion());
      assertFalse(stream.isDeleted());
    } else {
      assertNull(stream);
    }
  }

  /**
   * Subroutine to verify method getStream(name, ignoreDeletedFlag) behavior.
   *
   * @param tenantDesc Target tenant descriptor.
   * @param expectReturn Whether the tried getStream() would return a non-null result.
   * @param key The key to specify the stream.
   * @param ignoreDeletedFlag The ignoreDeletedFlag to specify.
   * @param streamName Expected stream name. Not used when expectedReturn is false.
   * @param version Expected version. Not used when expectedReturns is false.
   * @param isDeleted Expected soft deletion flag.
   */
  private void verifyGetLatestStream(
      TenantDesc tenantDesc,
      boolean expectReturn,
      String key,
      boolean ignoreDeletedFlag,
      String streamName,
      Long version,
      boolean isDeleted) {
    final StreamDesc stream = tenantDesc.getStream(key, ignoreDeletedFlag);
    if (expectReturn) {
      assertNotNull(stream);
      assertEquals(streamName, stream.getName());
      assertEquals(version, stream.getVersion());
      assertEquals(isDeleted, stream.isDeleted());
    } else {
      assertNull(stream);
    }
  }

  /**
   * Subroutine to verify method getStream(name, version, ignoreDeletedFlag) behavior.
   *
   * @param tenantDesc Target tenant descriptor.
   * @param expectReturn Whether the tried getStream() would return a non-null result.
   * @param key The key to specify the stream.
   * @param version The version to specify.
   * @param ignoreDeletedFlag The ignoreDeletedFlag to specify.
   * @param streamName Expected stream name. Not used when expectedReturn is false.
   * @param version Expected version. Not used when expectedReturns is false.
   * @param isDeleted Expected soft deletion flag.
   */
  private void verifyGetsWithVersion(
      TenantDesc tenantDesc,
      boolean expectReturn,
      String key,
      Long version,
      boolean ignoreDeletedFlag,
      String streamName,
      boolean isDeleted) {
    final StreamDesc stream = tenantDesc.getStream(key, version, ignoreDeletedFlag);
    if (expectReturn) {
      assertNotNull(stream);
      assertEquals(streamName, stream.getName());
      assertEquals(version, stream.getVersion());
      assertEquals(isDeleted, stream.isDeleted());
    } else {
      assertNull(stream);
    }
  }
}
