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
package io.isima.bios.data.impl.storage;

import static org.junit.Assert.assertEquals;

import com.datastax.driver.core.utils.UUIDs;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.common.ExtractState;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.StreamType;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SignalCassStreamTest {

  private static CassTenant dummyTenant;
  private static StreamDesc streamDesc;
  private static CassStream cassStream;
  private static String keyspaceName;
  private static String tableName;

  private static String indexingIntervalSave;

  @BeforeClass
  public static void setUpClass() throws ApplicationException {
    indexingIntervalSave = System.getProperty(TfosConfig.INDEXING_DEFAULT_INTERVAL_SECONDS);
    System.setProperty(TfosConfig.INDEXING_DEFAULT_INTERVAL_SECONDS, "3600");
    dummyTenant = new CassTenant(new TenantDesc("dummy", System.currentTimeMillis(), false));
    streamDesc = new StreamDesc("extract_test", 1533900000000L);
    streamDesc.setType(StreamType.SIGNAL);
    final List<AttributeDesc> attributeDescs = new ArrayList<>();
    attributeDescs.add(new AttributeDesc("one", InternalAttributeType.STRING));
    attributeDescs.add(new AttributeDesc("two", InternalAttributeType.UUID));
    streamDesc.setAttributes(attributeDescs);
    cassStream = CassStream.create(dummyTenant, streamDesc, null);
    keyspaceName = dummyTenant.getKeyspaceName();
    tableName = cassStream.getTableName();
  }

  @AfterClass
  public static void tearDownClass() {
    if (indexingIntervalSave != null) {
      System.setProperty(TfosConfig.INDEXING_DEFAULT_INTERVAL_SECONDS, indexingIntervalSave);
    } else {
      System.clearProperty(TfosConfig.INDEXING_DEFAULT_INTERVAL_SECONDS);
    }
  }

  @Test
  public void testOneHourWindow() throws ApplicationException, TfosException {
    List<CassAttributeDesc> attributes = new ArrayList<>();
    attributes.add(cassStream.getAttributeDesc("two"));
    attributes.add(cassStream.getAttributeDesc("one"));

    // since 2018/8/11 00:00:00.000 UTC for 1 hour
    final long startTime = 1533945600000L;
    final long endTime = 1533949200000L;
    ExtractState state =
        new ExtractState(null, cassStream.getTenantName(), cassStream.getStreamName(), null);
    final List<QueryInfo> queries =
        cassStream.makeExtractStatements(state, attributes, startTime, endTime);
    assertEquals(1, queries.size());
    assertEquals(
        String.format(
            "SELECT event_id,evt_two,evt_one FROM %s.evt_7568f22aada8382d8c93669a62ae34b3 "
                + "WHERE time_index = 1533945600000",
            dummyTenant.getKeyspaceName()),
        queries.get(0).getStatement().toString());
  }

  @Test
  public void testOneHourWindowReversed() throws ApplicationException, TfosException {
    List<CassAttributeDesc> attributes = new ArrayList<>();
    attributes.add(cassStream.getAttributeDesc("one"));
    attributes.add(cassStream.getAttributeDesc("two"));

    // since 2018/8/11 01:00:00.000 UTC for -1 hour
    final long startTime = 1533949200000L;
    final long endTime = 1533945600000L;
    ExtractState state =
        new ExtractState(null, cassStream.getTenantName(), cassStream.getStreamName(), null);
    final List<QueryInfo> queries =
        cassStream.makeExtractStatements(state, attributes, startTime, endTime);
    assertEquals(1, queries.size());
    assertEquals(
        String.format(
            "SELECT event_id,evt_one,evt_two FROM %s.%s WHERE time_index = 1533945600000",
            keyspaceName, tableName),
        queries.get(0).getStatement().toString());
  }

  @Test
  public void testOneHourWindowRightAttached() throws ApplicationException, TfosException {
    List<CassAttributeDesc> attributes = new ArrayList<>();
    attributes.add(cassStream.getAttributeDesc("two"));
    attributes.add(cassStream.getAttributeDesc("one"));
    // since 2018/8/11 00:00:00.001 UTC for 1 hour minus 1 ms
    final long startTime = 1533945600001L;
    final long endTime = 1533949200000L;
    ExtractState state =
        new ExtractState(null, cassStream.getTenantName(), cassStream.getStreamName(), null);
    final List<QueryInfo> queries =
        cassStream.makeExtractStatements(state, attributes, startTime, endTime);
    assertEquals(1, queries.size());
    assertEquals(
        String.format(
            "SELECT event_id,evt_two,evt_one FROM %s.%s"
                + " WHERE time_index = 1533945600000 AND event_id >= %s",
            keyspaceName, tableName, UUIDs.startOf(startTime)),
        queries.get(0).getStatement().toString());
    assertEquals(
        UUIDs.unixTimestamp(UUID.fromString("7d6ee710-9cf9-11e8-8080-808080808080")), startTime);
  }

  @Test
  public void testOneHourWindowLeftAttached() throws ApplicationException, TfosException {
    List<CassAttributeDesc> attributes = new ArrayList<>();
    attributes.add(cassStream.getAttributeDesc("two"));
    attributes.add(cassStream.getAttributeDesc("one"));
    // since 2018/8/11 00:00:00.000 UTC for 1 hour minus 1 ms
    final long startTime = 1533945600000L;
    final long endTime = 1533949199999L;
    ExtractState state =
        new ExtractState(null, cassStream.getTenantName(), cassStream.getStreamName(), null);
    final List<QueryInfo> queries =
        cassStream.makeExtractStatements(state, attributes, startTime, endTime);
    assertEquals(1, queries.size());
    assertEquals(
        String.format(
            "SELECT event_id,evt_two,evt_one FROM %s.%s"
                + " WHERE time_index = 1533945600000 AND event_id < %s",
            keyspaceName, tableName, UUIDs.startOf(endTime)),
        queries.get(0).getStatement().toString());
  }

  @Test
  public void testWithinOneHourWindow() throws ApplicationException, TfosException {
    List<CassAttributeDesc> attributes = new ArrayList<>();
    attributes.add(cassStream.getAttributeDesc("two"));
    attributes.add(cassStream.getAttributeDesc("one"));
    // since 2018/8/11 00:00:00.000 UTC for 1 hour minus 1 ms
    final long startTime = 1533945600001L;
    final long endTime = 1533949199999L;
    ExtractState state =
        new ExtractState(null, cassStream.getTenantName(), cassStream.getStreamName(), null);
    final List<QueryInfo> queries =
        cassStream.makeExtractStatements(state, attributes, startTime, endTime);
    assertEquals(1, queries.size());
    assertEquals(
        String.format(
            "SELECT event_id,evt_two,evt_one FROM %s.%s "
                + "WHERE time_index = 1533945600000 AND event_id >= %s AND event_id < %s",
            keyspaceName, tableName, UUIDs.startOf(startTime), UUIDs.startOf(endTime)),
        queries.get(0).getStatement().toString());
  }

  @Test
  public void testTwoWindowsBegin() throws ApplicationException, TfosException {
    List<CassAttributeDesc> attributes = new ArrayList<>();
    attributes.add(cassStream.getAttributeDesc("two"));
    attributes.add(cassStream.getAttributeDesc("one"));
    // since 2018/8/11 00:00:00.001 UTC for 1 hour
    final long startTime = 1533945600001L;
    final long endTime = 1533949200001L;
    ExtractState state =
        new ExtractState(null, cassStream.getTenantName(), cassStream.getStreamName(), null);
    final List<QueryInfo> queries =
        cassStream.makeExtractStatements(state, attributes, startTime, endTime);
    assertEquals(2, queries.size());
    assertEquals(
        String.format(
            "SELECT event_id,evt_two,evt_one FROM %s.%s"
                + " WHERE time_index = 1533945600000 AND event_id >= %s",
            keyspaceName, tableName, UUIDs.startOf(startTime)),
        queries.get(0).getStatement().toString());
    assertEquals(
        String.format(
            "SELECT event_id,evt_two,evt_one FROM %s.%s"
                + " WHERE time_index = 1533949200000 AND event_id < %s",
            keyspaceName, tableName, UUIDs.startOf(endTime)),
        queries.get(1).getStatement().toString());
  }

  @Test
  public void testTwoWindowsEnd() throws ApplicationException, TfosException {
    List<CassAttributeDesc> attributes = new ArrayList<>();
    attributes.add(cassStream.getAttributeDesc("two"));
    attributes.add(cassStream.getAttributeDesc("one"));
    // since 2018/8/11 00:59:59.999 UTC for 1 hour
    final long startTime = 1533949199999L;
    final long endTime = 1533952799999L;
    ExtractState state =
        new ExtractState(null, cassStream.getTenantName(), cassStream.getStreamName(), null);
    final List<QueryInfo> queries =
        cassStream.makeExtractStatements(state, attributes, startTime, endTime);
    assertEquals(2, queries.size());
    assertEquals(
        String.format(
            "SELECT event_id,evt_two,evt_one FROM %s.%s"
                + " WHERE time_index = 1533945600000 AND event_id >= %s",
            keyspaceName, tableName, UUIDs.startOf(startTime)),
        queries.get(0).getStatement().toString());
    assertEquals(
        String.format(
            "SELECT event_id,evt_two,evt_one FROM %s.%s"
                + " WHERE time_index = 1533949200000 AND event_id < %s",
            keyspaceName, tableName, UUIDs.startOf(endTime)),
        queries.get(1).getStatement().toString());
  }

  @Test
  public void testThreeWindowsBegin() throws ApplicationException, TfosException {
    List<CassAttributeDesc> attributes = new ArrayList<>();
    attributes.add(cassStream.getAttributeDesc("two"));
    attributes.add(cassStream.getAttributeDesc("one"));
    // since 2018/8/11 00:00:00.001 UTC for 2 hour
    final long startTime = 1533945600001L;
    final long endTime = 1533952800001L;
    ExtractState state =
        new ExtractState(null, cassStream.getTenantName(), cassStream.getStreamName(), null);
    final List<QueryInfo> queries =
        cassStream.makeExtractStatements(state, attributes, startTime, endTime);
    assertEquals(3, queries.size());
    assertEquals(
        String.format(
            "SELECT event_id,evt_two,evt_one FROM %s.%s"
                + " WHERE time_index = 1533945600000 AND event_id >= %s",
            keyspaceName, tableName, UUIDs.startOf(startTime)),
        queries.get(0).getStatement().toString());
    assertEquals(
        String.format(
            "SELECT event_id,evt_two,evt_one FROM %s.%s WHERE time_index = 1533949200000",
            keyspaceName, tableName),
        queries.get(1).getStatement().toString());
    assertEquals(
        String.format(
            "SELECT event_id,evt_two,evt_one FROM %s.%s"
                + " WHERE time_index = 1533952800000 AND event_id < %s",
            keyspaceName, tableName, UUIDs.startOf(endTime)),
        queries.get(2).getStatement().toString());
  }

  @Test
  public void testThreeWindowsEnd() throws ApplicationException, TfosException {
    List<CassAttributeDesc> attributes = new ArrayList<>();
    attributes.add(cassStream.getAttributeDesc("two"));
    attributes.add(cassStream.getAttributeDesc("one"));
    // since 2018/8/11 00:59:59.999 UTC for 2 hour
    final long startTime = 1533949199999L;
    final long endTime = 1533956399999L;
    ExtractState state =
        new ExtractState(null, cassStream.getTenantName(), cassStream.getStreamName(), null);
    final List<QueryInfo> queries =
        cassStream.makeExtractStatements(state, attributes, startTime, endTime);
    assertEquals(3, queries.size());
    assertEquals(
        String.format(
            "SELECT event_id,evt_two,evt_one FROM %s.%s"
                + " WHERE time_index = 1533945600000 AND event_id >= %s",
            keyspaceName, tableName, UUIDs.startOf(startTime)),
        queries.get(0).getStatement().toString());
    assertEquals(
        String.format(
            "SELECT event_id,evt_two,evt_one FROM %s.%s WHERE time_index = 1533949200000",
            keyspaceName, tableName),
        queries.get(1).getStatement().toString());
    assertEquals(
        String.format(
            "SELECT event_id,evt_two,evt_one FROM %s.%s"
                + " WHERE time_index = 1533952800000 AND event_id < %s",
            keyspaceName, tableName, UUIDs.startOf(endTime)),
        queries.get(2).getStatement().toString());
  }

  @Test
  public void testZeroWindowLengthOnEdge() throws ApplicationException, TfosException {
    List<CassAttributeDesc> attributes = new ArrayList<>();
    attributes.add(cassStream.getAttributeDesc("two"));
    attributes.add(cassStream.getAttributeDesc("one"));
    // since 2018/8/11 00:00:00.001 UTC for 0 ms
    final long startTime = 1533945600000L;
    ExtractState state =
        new ExtractState(null, cassStream.getTenantName(), cassStream.getStreamName(), null);
    final List<QueryInfo> queries =
        cassStream.makeExtractStatements(state, attributes, startTime, startTime);
    assertEquals(0, queries.size());
  }

  @Test
  public void testZeroWindowLengthOffEdge() throws ApplicationException, TfosException {
    List<CassAttributeDesc> attributes = new ArrayList<>();
    attributes.add(cassStream.getAttributeDesc("two"));
    attributes.add(cassStream.getAttributeDesc("one"));
    // since 2018/8/11 00:00:00.001 UTC for 0 ms
    final long startTime = 1533945600001L;
    ExtractState state =
        new ExtractState(null, cassStream.getTenantName(), cassStream.getStreamName(), null);
    final List<QueryInfo> queries =
        cassStream.makeExtractStatements(state, attributes, startTime, startTime);
    assertEquals(0, queries.size());
  }
}
