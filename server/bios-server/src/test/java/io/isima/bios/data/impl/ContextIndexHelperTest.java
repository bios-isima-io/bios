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

import static io.isima.bios.admin.v1.AdminConstants.CONTEXT_AUDIT_ATTRIBUTE_OPERATION;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.models.IndexType;
import io.isima.bios.models.MissingAttributePolicyV1;
import io.isima.bios.models.Range;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.FeatureDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.utils.StringUtils;
import java.util.Date;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class ContextIndexHelperTest {

  private StreamDesc contextDesc;
  private FeatureDesc featureDesc;

  @Before
  public void setUp() {
    contextDesc = new StreamDesc("test", StreamType.CONTEXT);
    contextDesc.setMissingValuePolicy(MissingAttributePolicyV1.STRICT);
    contextDesc.addAttribute(new AttributeDesc("productId", InternalAttributeType.STRING));
    contextDesc.addAttribute(new AttributeDesc("productName", InternalAttributeType.STRING));
    contextDesc.addAttribute(new AttributeDesc("price", InternalAttributeType.DOUBLE));
    contextDesc.addAttribute(new AttributeDesc("quantity", InternalAttributeType.DOUBLE));
    contextDesc.setPrimaryKey(List.of("productId"));
    featureDesc = new FeatureDesc();
    featureDesc.setName("byQuantity");
    featureDesc.setDimensions(List.of("quantity"));
    featureDesc.setAttributes(List.of("productName"));
    featureDesc.setFeatureInterval(5000L);
    featureDesc.setIndexed(Boolean.TRUE);
    featureDesc.setIndexType(IndexType.RANGE_QUERY);
    contextDesc.setFeatures(List.of(featureDesc));
    contextDesc.setAuditEnabled(true);
  }

  @Test
  public void simpleIndexCollection() {
    long start = System.currentTimeMillis();
    final var auditEvents =
        List.of(
            makeAuditEvent("Insert", "product1", "one", 10.23, 20, "product1", "one", 10.23, 20),
            makeAuditEvent("Insert", "product2", "two", 3.15, 10, "product2", "two", 3.15, 10),
            makeAuditEvent("Delete", "product3", "three", 4.8, 7, "product3", "three", 4.8, 7),
            makeAuditEvent("Update", "product4", "four", 6.2, 9, "product4", "yon", 5.3, 11));
    long end = System.currentTimeMillis() + 1;

    final var range = new Range(start, end);

    final var indexEntries =
        ContextIndexHelper.collectIndexEntries(contextDesc, featureDesc, range, auditEvents, null);
    final var toAdd = indexEntries.get("new");
    final var toDelete = indexEntries.get("deleted");
    assertThat(toAdd.size(), is(3));
    assertThat(toAdd.get(0).get("productId"), is("product1"));
    assertThat(toAdd.get(0).get("productName"), is("one"));
    assertThat(toAdd.get(0).get("quantity"), is(20));
    assertThat(toAdd.get(1).get("productId"), is("product2"));
    assertThat(toAdd.get(1).get("productName"), is("two"));
    assertThat(toAdd.get(1).get("quantity"), is(10));
    assertThat(toAdd.get(2).get("productId"), is("product4"));
    assertThat(toAdd.get(2).get("productName"), is("four"));
    assertThat(toAdd.get(2).get("quantity"), is(9));

    assertThat(toDelete.size(), is(2));
    assertThat(toDelete.get(0).get("productId"), is("product3"));
    assertThat(toDelete.get(0).get("quantity"), is(7));
    assertThat(toDelete.get(1).get("productId"), is("product4"));
    assertThat(toDelete.get(1).get("quantity"), is(11));
  }

  @Test
  public void modButNoIndexChange() {
    long start = System.currentTimeMillis();
    final var auditEvents =
        List.of(
            makeAuditEvent("Insert", "product1", "one", 10.23, 20, "product1", "one", 10.23, 20),
            makeAuditEvent("Update", "product1", "one", 12.23, 20, "product1", "one", 10.23, 20));
    long end = System.currentTimeMillis() + 1;

    final var range = new Range(start, end);

    final var indexEntries =
        ContextIndexHelper.collectIndexEntries(contextDesc, featureDesc, range, auditEvents, null);
    final var toAdd = indexEntries.get("new");
    final var toDelete = indexEntries.get("deleted");
    assertThat(toAdd.size(), is(1));
    assertThat(toAdd.get(0).get("productId"), is("product1"));
    assertThat(toAdd.get(0).get("productName"), is("one"));
    assertThat(toAdd.get(0).get("quantity"), is(20));

    assertThat(toDelete.size(), is(0));
  }

  @Test
  public void modIndexAttribute() {
    long start = System.currentTimeMillis();
    final var auditEvents =
        List.of(
            makeAuditEvent("Insert", "product1", "one", 10.23, 20, "product1", "one", 10.23, 20),
            makeAuditEvent(
                "Update", "product1", "one.five", 12.23, 20, "product1", "one", 10.23, 20));
    long end = System.currentTimeMillis() + 1;

    final var range = new Range(start, end);

    final var indexEntries =
        ContextIndexHelper.collectIndexEntries(contextDesc, featureDesc, range, auditEvents, null);
    final var toAdd = indexEntries.get("new");
    final var toDelete = indexEntries.get("deleted");
    assertThat(toAdd.size(), is(1));
    assertThat(toAdd.get(0).get("productId"), is("product1"));
    assertThat(toAdd.get(0).get("productName"), is("one.five"));
    assertThat(toAdd.get(0).get("quantity"), is(20));

    assertThat(toDelete.size(), is(0));
  }

  @Test
  public void modIndexKeyTwice() {
    long start = System.currentTimeMillis();
    final var auditEvents =
        List.of(
            makeAuditEvent(
                "Update", "product1", "one.two", 10.23, 21, "product1", "one", 10.23, 20),
            makeAuditEvent(
                "Update", "product1", "one.five", 12.23, 18, "product1", "one.two", 10.23, 21));
    long end = System.currentTimeMillis() + 1;

    final var range = new Range(start, end);

    final var indexEntries =
        ContextIndexHelper.collectIndexEntries(contextDesc, featureDesc, range, auditEvents, null);
    final var toAdd = indexEntries.get("new");
    final var toDelete = indexEntries.get("deleted");
    assertThat(toAdd.size(), is(1));
    assertThat(toAdd.get(0).get("productId"), is("product1"));
    assertThat(toAdd.get(0).get("productName"), is("one.five"));
    assertThat(toAdd.get(0).get("quantity"), is(18));

    assertThat(toDelete.size(), is(1));
    assertThat(toDelete.get(0).get("productId"), is("product1"));
    assertThat(toDelete.get(0).get("quantity"), is(20));
  }

  @Test
  public void deleteAndInsert() {
    long start = System.currentTimeMillis();
    final var auditEvents =
        List.of(
            makeAuditEvent("Delete", "product1", "one", 10.23, 20, "product1", "one", 10.23, 20),
            makeAuditEvent(
                "Insert", "product1", "one.two", 12.23, 18, "product1", "one.two", 10.23, 18));
    long end = System.currentTimeMillis() + 1;

    final var range = new Range(start, end);

    final var indexEntries =
        ContextIndexHelper.collectIndexEntries(contextDesc, featureDesc, range, auditEvents, null);
    final var toAdd = indexEntries.get("new");
    final var toDelete = indexEntries.get("deleted");
    assertThat(toAdd.size(), is(1));
    assertThat(toAdd.get(0).get("productId"), is("product1"));
    assertThat(toAdd.get(0).get("quantity"), is(18));

    assertThat(toDelete.size(), is(1));
    assertThat(toDelete.get(0).get("productId"), is("product1"));
    assertThat(toDelete.get(0).get("quantity"), is(20));
  }

  @Test
  public void deleteInsertAndDelete() {
    long start = System.currentTimeMillis();
    final var auditEvents =
        List.of(
            makeAuditEvent("Delete", "product1", "one", 10.23, 20, "product1", "one", 10.23, 20),
            makeAuditEvent(
                "Insert", "product1", "one.two", 12.23, 18, "product1", "one.two", 10.23, 18),
            makeAuditEvent(
                "Delete", "product1", "one.two", 12.23, 18, "product1", "one.two", 10.23, 18));
    long end = System.currentTimeMillis() + 1;

    final var range = new Range(start, end);

    final var indexEntries =
        ContextIndexHelper.collectIndexEntries(contextDesc, featureDesc, range, auditEvents, null);
    final var toAdd = indexEntries.get("new");
    final var toDelete = indexEntries.get("deleted");
    assertThat(toAdd.size(), is(0));

    assertThat(toDelete.size(), is(1));
    assertThat(toDelete.get(0).get("productId"), is("product1"));
    assertThat(toDelete.get(0).get("quantity"), is(20));
  }

  @Test
  public void deleteAndInsertAndModWithoutKeyChange() {
    long start = System.currentTimeMillis();
    final var auditEvents =
        List.of(
            makeAuditEvent("Delete", "product1", "one", 10.23, 20, "product1", "one", 10.23, 20),
            makeAuditEvent(
                "Insert", "product1", "one.two", 12.23, 18, "product1", "one.two", 10.23, 18),
            makeAuditEvent(
                "Insert", "product1", "one.five", 12.23, 18, "product1", "one.two", 10.23, 18));
    long end = System.currentTimeMillis() + 1;

    final var range = new Range(start, end);

    final var indexEntries =
        ContextIndexHelper.collectIndexEntries(contextDesc, featureDesc, range, auditEvents, null);
    final var toAdd = indexEntries.get("new");
    final var toDelete = indexEntries.get("deleted");
    assertThat(toAdd.size(), is(1));
    assertThat(toAdd.get(0).get("productId"), is("product1"));
    assertThat(toAdd.get(0).get("productName"), is("one.five"));
    assertThat(toAdd.get(0).get("quantity"), is(18));

    assertThat(toDelete.size(), is(1));
    assertThat(toDelete.get(0).get("productId"), is("product1"));
    assertThat(toDelete.get(0).get("quantity"), is(20));
  }

  @Test
  public void deleteAndInsertAndModWithKeyChange() {
    long start = System.currentTimeMillis();
    final var auditEvents =
        List.of(
            makeAuditEvent("Delete", "product1", "one", 10.23, 20, "product1", "one", 10.23, 20),
            makeAuditEvent(
                "Insert", "product1", "one.two", 12.23, 18, "product1", "one.two", 10.23, 18),
            makeAuditEvent(
                "Insert", "product1", "one.six", 12.23, 24, "product1", "one.two", 10.23, 18));
    long end = System.currentTimeMillis() + 1;

    final var range = new Range(start, end);

    final var indexEntries =
        ContextIndexHelper.collectIndexEntries(contextDesc, featureDesc, range, auditEvents, null);
    final var toAdd = indexEntries.get("new");
    final var toDelete = indexEntries.get("deleted");
    assertThat(toAdd.size(), is(1));
    assertThat(toAdd.get(0).get("productId"), is("product1"));
    assertThat(toAdd.get(0).get("productName"), is("one.six"));
    assertThat(toAdd.get(0).get("quantity"), is(24));

    assertThat(toDelete.size(), is(1));
    assertThat(toDelete.get(0).get("productId"), is("product1"));
    assertThat(toDelete.get(0).get("quantity"), is(20));
  }

  @Test
  public void insertAndDelete() {
    long start = System.currentTimeMillis();
    final var auditEvents =
        List.of(
            makeAuditEvent("Insert", "product1", "one", 12.23, 18, "product1", "one", 10.23, 18),
            makeAuditEvent("Delete", "product1", "one", 12.23, 18, "product1", "one", 10.23, 18));
    long end = System.currentTimeMillis() + 1;

    final var range = new Range(start, end);

    final var indexEntries =
        ContextIndexHelper.collectIndexEntries(contextDesc, featureDesc, range, auditEvents, null);
    final var toAdd = indexEntries.get("new");
    final var toDelete = indexEntries.get("deleted");
    assertThat(toAdd.size(), is(0));

    assertThat(toDelete.size(), is(0));
  }

  @Test
  public void updateWithoutKeyChangeAndDelete() {
    long start = System.currentTimeMillis();
    final var auditEvents =
        List.of(
            makeAuditEvent(
                "Update", "product1", "one.two", 12.23, 23, "product1", "one", 10.20, 23),
            makeAuditEvent(
                "Delete", "product1", "one.two", 12.23, 23, "product1", "one.two", 10.23, 23));
    long end = System.currentTimeMillis() + 1;

    final var range = new Range(start, end);

    final var indexEntries =
        ContextIndexHelper.collectIndexEntries(contextDesc, featureDesc, range, auditEvents, null);
    final var toAdd = indexEntries.get("new");
    final var toDelete = indexEntries.get("deleted");
    assertThat(toAdd.size(), is(0));

    assertThat(toDelete.size(), is(1));
    assertThat(toDelete.get(0).get("productId"), is("product1"));
    assertThat(toDelete.get(0).get("quantity"), is(23));
  }

  @Test
  public void updateWithoutIndexChangeAndDelete() {
    long start = System.currentTimeMillis();
    final var auditEvents =
        List.of(
            makeAuditEvent("Update", "product1", "one", 12.23, 23, "product1", "one", 10.20, 23),
            makeAuditEvent("Delete", "product1", "one", 12.23, 23, "product1", "one", 12.23, 23));
    long end = System.currentTimeMillis() + 1;

    final var range = new Range(start, end);

    final var indexEntries =
        ContextIndexHelper.collectIndexEntries(contextDesc, featureDesc, range, auditEvents, null);
    final var toAdd = indexEntries.get("new");
    final var toDelete = indexEntries.get("deleted");
    assertThat(toAdd.size(), is(0));

    assertThat(toDelete.size(), is(1));
    assertThat(toDelete.get(0).get("productId"), is("product1"));
    assertThat(toDelete.get(0).get("quantity"), is(23));
  }

  @Test
  public void updateWithKeyChangeAndDelete() {
    long start = System.currentTimeMillis();
    final var auditEvents =
        List.of(
            makeAuditEvent("Update", "product1", "one", 12.23, 23, "product1", "one", 10.20, 20),
            makeAuditEvent("Delete", "product1", "one", 12.23, 23, "product1", "one", 10.23, 23));
    long end = System.currentTimeMillis() + 1;

    final var range = new Range(start, end);

    final var indexEntries =
        ContextIndexHelper.collectIndexEntries(contextDesc, featureDesc, range, auditEvents, null);
    final var toAdd = indexEntries.get("new");
    final var toDelete = indexEntries.get("deleted");
    assertThat(toAdd.size(), is(0));

    assertThat(toDelete.size(), is(1));
    assertThat(toDelete.get(0).get("productId"), is("product1"));
    assertThat(toDelete.get(0).get("quantity"), is(20));
  }

  private Event makeAuditEvent(Object... values) {
    final var event = new EventJson();
    event.set(CONTEXT_AUDIT_ATTRIBUTE_OPERATION, values[0]);
    event.setIngestTimestamp(new Date(System.currentTimeMillis()));
    final int numAttributes = contextDesc.getAttributes().size();
    for (int i = 0; i < numAttributes; ++i) {
      final var attributeName = contextDesc.getAttributes().get(i).getName();
      final var prevAttributeName = StringUtils.prefixToCamelCase("prev", attributeName);
      final var value = i + 1 < values.length ? values[i + 1] : null;
      final var prevValue =
          numAttributes + i + 1 < values.length ? values[numAttributes + i + 1] : null;
      event.set(attributeName, value);
      event.set(prevAttributeName, prevValue);
    }
    return event;
  }
}
