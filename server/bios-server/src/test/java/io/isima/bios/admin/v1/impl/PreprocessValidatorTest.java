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
package io.isima.bios.admin.v1.impl;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.IngestState;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.metrics.MetricsStreamProvider;
import io.isima.bios.models.MissingAttributePolicyV1;
import io.isima.bios.models.ProcessStage;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.v1.ActionDesc;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.PreprocessDesc;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.preprocess.JoinProcessor;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class PreprocessValidatorTest {

  private static ObjectMapper objectMapper;
  private static String tenantName;

  private AdminImpl admin;

  private StreamConfig cityContext;
  private StreamConfig productContext;
  private StreamConfig inventoryContext;
  private StreamConfig counterContext;
  private StreamConfig brandContext;
  private StreamConfig signal;
  private StreamConfig orderRecord;

  @BeforeClass
  public static void setUpClass() {
    objectMapper = TfosObjectMapperProvider.get();
    tenantName = "preprocess_validator_test";
  }

  @Before
  public void setUp() throws Exception {
    admin = new AdminImpl(null, new MetricsStreamProvider());
    final TenantConfig tenant = new TenantConfig(tenantName);

    final Long timestamp = System.currentTimeMillis();
    admin.addTenant(tenant, RequestPhase.INITIAL, timestamp);
    admin.addTenant(tenant, RequestPhase.FINAL, timestamp);

    final String cityContextSrc =
        "{"
            + "  'name': 'city_context',"
            + "  'type': 'context',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "     {'name': 'city_id', 'type': 'int'},"
            + "     {'name': 'city_name', 'type': 'string'},"
            + "     {'name': 'supplier_city_id', 'type': 'int'},"
            + "     {'name': 'factory_city_id', 'type': 'int'}"
            + "  ]"
            + "}";

    cityContext =
        objectMapper
            .readValue(cityContextSrc.replace("'", "\""), StreamConfig.class)
            .setVersion(System.currentTimeMillis());

    final String productContextSrc =
        "{"
            + "  'name': 'product_context',"
            + "  'type': 'context',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'product_id', 'type': 'int'},"
            + "    {'name': 'product_name', 'type': 'string'},"
            + "    {'name': 'product_brand_id', 'type': 'int'}"
            + "  ]"
            + "}";

    productContext =
        objectMapper
            .readValue(productContextSrc.replace("'", "\""), StreamConfig.class)
            .setVersion(System.currentTimeMillis());

    final String brandContextSrc =
        "{"
            + "  'name': 'brand_context',"
            + "  'type': 'context',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'brand_id', 'type': 'int'},"
            + "    {'name': 'brand_name', 'type': 'string'},"
            + "    {'name': 'headquaters_city_id', 'type': 'int'}"
            + "  ]"
            + "}";

    brandContext =
        objectMapper
            .readValue(brandContextSrc.replace("'", "\""), StreamConfig.class)
            .setVersion(System.currentTimeMillis());

    final String productInventoryContextSrc =
        "{"
            + "  'name': 'inventory_context',"
            + "  'type': 'context',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'product_id', 'type': 'int'},"
            + "    {'name': 'inventory', 'type': 'int'},"
            + "    {'name': 'warehouse_city_id', 'type': 'int'}"
            + "  ]"
            + "}";

    inventoryContext =
        objectMapper
            .readValue(productInventoryContextSrc.replace("'", "\""), StreamConfig.class)
            .setVersion(System.currentTimeMillis());

    final String counterContextSrc =
        "{"
            + "  'name': 'inventoryCounter',"
            + "  'type': 'context',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'fcType', 'type': 'string'},"
            + "    {'name': 'fcId', 'type': 'long'},"
            + "    {'name': 'itemId', 'type': 'long'},"
            + "    {'name': 'onHandValue', 'type': 'Double'},"
            + "    {'name': 'demandValue', 'type': 'Double'}"
            + "  ],"
            + "  'primaryKey': ['fcType', 'fcId', 'itemId']"
            + "}";

    counterContext =
        objectMapper
            .readValue(counterContextSrc.replace("'", "\""), StreamConfig.class)
            .setVersion(System.currentTimeMillis());

    final String signalSrc =
        "{"
            + "  'name': 'order_signal',"
            + "  'type': 'signal',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'order_id', 'type': 'int'},"
            + "    {'name': 'city_id', 'type': 'int'}"
            + "  ],"
            + "  'preprocesses': ["
            + "    {"
            + "      'name': 'city_preprocess_1',"
            + "      'condition': 'city_id',"
            + "      'missingLookupPolicy': 'strict',"
            + "      'actions': ["
            + "        {"
            + "          'actionType': 'MERGE',"
            + "          'context': 'city_context',"
            + "          'attribute': 'city_name',"
            + "          'as': 'city_name'"
            + "        }, {"
            + "          'actionType': 'MERGE',"
            + "          'context': 'city_context',"
            + "          'attribute': 'supplier_city_id',"
            + "          'as': 'supplier_city_id'"
            + "        }"
            + "      ]"
            + "    },"
            + "    {"
            + "      'name': 'city_preprocess_2',"
            + "      'condition': 'supplier_city_id',"
            + "      'missingLookupPolicy': 'strict',"
            + "      'actions': ["
            + "        {"
            + "          'actionType': 'MERGE',"
            + "          'context': 'city_context',"
            + "          'attribute': 'city_name',"
            + "          'as': 'supplier_city_name'"
            + "        }"
            + "      ]"
            + "    }"
            + "  ],"
            + "  'views': [],"
            + "  'postprocesses': []"
            + "}";

    signal =
        objectMapper
            .readValue(signalSrc.replace("'", "\""), StreamConfig.class)
            .setVersion(System.currentTimeMillis());

    final String orderRecordSrc =
        "{"
            + "  'name': 'orderRecord',"
            + "  'type': 'signal',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'orderId', 'type': 'long'},"
            + "    {'name': 'fulfillmentCenterType', 'type': 'string'},"
            + "    {'name': 'fulfillmentCenterId', 'type': 'long'},"
            + "    {'name': 'productId', 'type': 'long'},"
            + "    {'name': 'productCategory', 'type': 'string'}"
            + "  ],"
            + "  'preprocesses': ["
            + "    {"
            + "      'name': 'byProduct',"
            + "      'foreignKey': ['fulfillmentCenterType', 'fulfillmentCenterId', 'productId'],"
            + "      'missingLookupPolicy': 'strict',"
            + "      'actions': ["
            + "        {"
            + "          'actionType': 'MERGE',"
            + "          'context': 'inventoryCounter',"
            + "          'attribute': 'onHandValue',"
            + "          'as': 'onHand'"
            + "        }, {"
            + "          'actionType': 'MERGE',"
            + "          'context': 'inventoryCounter',"
            + "          'attribute': 'demandValue',"
            + "          'as': 'demand'"
            + "        }"
            + "      ]"
            + "    }"
            + "  ],"
            + "  'views': [],"
            + "  'postprocesses': []"
            + "}";

    orderRecord =
        objectMapper
            .readValue(orderRecordSrc.replace("'", "\""), StreamConfig.class)
            .setVersion(System.currentTimeMillis());
  }

  @Test
  public void testContextJoinChainingBasic() throws Exception {

    admin.addStream(tenantName, cityContext, RequestPhase.INITIAL);
    admin.addStream(tenantName, cityContext, RequestPhase.FINAL);

    admin.addStream(tenantName, signal, RequestPhase.INITIAL);
    admin.addStream(tenantName, signal, RequestPhase.FINAL);

    StreamDesc registeredSignal = admin.getStream(tenantName, signal.getName());
    // System.out.println(registeredSignal);
    final List<ProcessStage> stages = registeredSignal.getPreprocessStages();
    assertNotNull(stages);
    assertEquals(stages.toString(), 2, stages.size());
    assertEquals(stages.get(0).toString(), "city_context.[city_id]", stages.get(0).getName());
    final Function<IngestState, IngestState> process0 = stages.get(0).getProcess();
    String message = process0.toString();
    assertTrue(message, process0 instanceof JoinProcessor);
    final JoinProcessor mprocess0 = ((JoinProcessor) process0);
    assertEquals(message, List.of("city_id"), mprocess0.getForeignKeyNames());
    assertEquals(message, 2, mprocess0.getActions().size());
    assertEquals(message, "city_context", mprocess0.getActions().get(0).getContext());
    assertEquals(message, "city_name", mprocess0.getActions().get(0).getAttribute());
    assertEquals(message, "city_name", mprocess0.getActions().get(0).getAs());
    assertEquals(message, "city_context", mprocess0.getActions().get(1).getContext());
    assertEquals(message, "supplier_city_id", mprocess0.getActions().get(1).getAttribute());
    assertEquals(message, "supplier_city_id", mprocess0.getActions().get(1).getAs());

    assertEquals(stages.get(1).toString(), "city_context.[city_id]", stages.get(0).getName());
    final Function<IngestState, IngestState> process1 = stages.get(1).getProcess();
    message = process1.toString();
    assertTrue(message, process1 instanceof JoinProcessor);
    final JoinProcessor mprocess1 = ((JoinProcessor) process1);
    assertEquals(message, List.of("supplier_city_id"), mprocess1.getForeignKeyNames());
    assertEquals(message, 1, mprocess1.getActions().size());
    assertEquals(message, "city_context", mprocess1.getActions().get(0).getContext());
    assertEquals(message, "city_name", mprocess1.getActions().get(0).getAttribute());
    assertEquals(message, "supplier_city_name", mprocess1.getActions().get(0).getAs());
  }

  @Test
  public void testMultipleContexts() throws Exception {

    admin.addStream(tenantName, cityContext, RequestPhase.INITIAL);
    admin.addStream(tenantName, cityContext, RequestPhase.FINAL);
    admin.addStream(tenantName, productContext, RequestPhase.INITIAL);
    admin.addStream(tenantName, productContext, RequestPhase.FINAL);
    admin.addStream(tenantName, brandContext, RequestPhase.INITIAL);
    admin.addStream(tenantName, brandContext, RequestPhase.FINAL);
    admin.addStream(tenantName, inventoryContext, RequestPhase.INITIAL);
    admin.addStream(tenantName, inventoryContext, RequestPhase.FINAL);

    signal.addAttribute(new AttributeDesc("product_id", InternalAttributeType.INT));
    final String[] preprocessSrcs =
        new String[] {
          "{"
              + "  'name': 'join_product_info',"
              + "  'condition': 'product_id',"
              + "  'missingLookupPolicy': 'strict',"
              + "  'actions': ["
              + "    {"
              + "      'actionType': 'MERGE',"
              + "      'context': 'product_context',"
              + "      'attribute': 'product_name'"
              + "    }, {"
              + "      'actionType': 'MERGE',"
              + "      'context': 'product_context',"
              + "      'attribute': 'product_brand_id'"
              + "    }, {"
              + "      'actionType': 'MERGE',"
              + "      'context': 'inventory_context',"
              + "      'attribute': 'inventory'"
              + "    }, {"
              + "      'actionType': 'MERGE',"
              + "      'context': 'inventory_context',"
              + "      'attribute': 'warehouse_city_id'"
              + "    }"
              + "  ]"
              + "}",
          "{"
              + "  'name': 'join_brand_info',"
              + "  'condition': 'product_brand_id',"
              + "  'missingLookupPolicy': 'strict',"
              + "  'actions': ["
              + "    {"
              + "      'actionType': 'MERGE',"
              + "      'context': 'brand_context',"
              + "      'attribute': 'brand_name'"
              + "    }, {"
              + "      'actionType': 'MERGE',"
              + "      'context': 'brand_context',"
              + "      'attribute': 'headquaters_city_id'"
              + "    }"
              + "  ]"
              + "}",
          "{"
              + "  'name': 'join_warehouse_city',"
              + "  'condition': 'warehouse_city_id',"
              + "  'missingLookupPolicy': 'strict',"
              + "  'actions': ["
              + "    {"
              + "      'actionType': 'MERGE',"
              + "      'context': 'city_context',"
              + "      'attribute': 'city_name',"
              + "      'as': 'warehouse_city'"
              + "    }"
              + "  ]"
              + "}"
        };
    for (String src : preprocessSrcs) {
      signal.addPreprocess(objectMapper.readValue(src.replace("'", "\""), PreprocessDesc.class));
    }

    admin.addStream(tenantName, signal, RequestPhase.INITIAL);
    admin.addStream(tenantName, signal, RequestPhase.FINAL);

    StreamDesc registeredSignal = admin.getStream(tenantName, signal.getName());
    final List<ProcessStage> stages = registeredSignal.getPreprocessStages();
    assertNotNull(stages);
    assertEquals(stages.toString(), 6, stages.size());
    // System.out.println(stages);

    assertEquals(stages.get(0).toString(), "city_context.[city_id]", stages.get(0).getName());
    final Function<IngestState, IngestState> process0 = stages.get(0).getProcess();
    String message = process0.toString();
    assertTrue(message, process0 instanceof JoinProcessor);
    final JoinProcessor mprocess0 = ((JoinProcessor) process0);
    assertEquals(message, List.of("city_id"), mprocess0.getForeignKeyNames());
    assertEquals(message, 2, mprocess0.getActions().size());
    assertEquals(message, "city_context", mprocess0.getActions().get(0).getContext());
    assertEquals(message, "city_name", mprocess0.getActions().get(0).getAttribute());
    assertEquals(message, "city_name", mprocess0.getActions().get(0).getAs());
    assertEquals(message, "city_context", mprocess0.getActions().get(1).getContext());
    assertEquals(message, "supplier_city_id", mprocess0.getActions().get(1).getAttribute());
    assertEquals(message, "supplier_city_id", mprocess0.getActions().get(1).getAs());

    assertEquals(stages.get(1).toString(), "city_context.[city_id]", stages.get(0).getName());
    final Function<IngestState, IngestState> process1 = stages.get(1).getProcess();
    message = process1.toString();
    assertTrue(message, process1 instanceof JoinProcessor);
    final JoinProcessor mprocess1 = ((JoinProcessor) process1);
    assertEquals(message, List.of("supplier_city_id"), mprocess1.getForeignKeyNames());
    assertEquals(message, 1, mprocess1.getActions().size());
    assertEquals(message, "city_context", mprocess1.getActions().get(0).getContext());
    assertEquals(message, "city_name", mprocess1.getActions().get(0).getAttribute());
    assertEquals(message, "supplier_city_name", mprocess1.getActions().get(0).getAs());

    assertEquals(stages.get(2).toString(), "product_context.[product_id]", stages.get(2).getName());
    final Function<IngestState, IngestState> process2 = stages.get(2).getProcess();
    message = process2.toString();
    assertTrue(message, process2 instanceof JoinProcessor);
    final JoinProcessor mprocess2 = ((JoinProcessor) process2);
    assertEquals(message, List.of("product_id"), mprocess2.getForeignKeyNames());
    assertEquals(message, 2, mprocess2.getActions().size());
    assertEquals(message, "product_context", mprocess2.getActions().get(0).getContext());
    assertEquals(message, "product_name", mprocess2.getActions().get(0).getAttribute());
    assertNull(message, mprocess2.getActions().get(0).getAs());
    assertEquals(message, "product_context", mprocess2.getActions().get(1).getContext());
    assertEquals(message, "product_brand_id", mprocess2.getActions().get(1).getAttribute());
    assertNull(message, mprocess2.getActions().get(1).getAs());

    assertEquals(
        stages.get(3).toString(), "inventory_context.[product_id]", stages.get(3).getName());
    final Function<IngestState, IngestState> process3 = stages.get(3).getProcess();
    message = process3.toString();
    assertTrue(message, process3 instanceof JoinProcessor);
    final JoinProcessor mprocess3 = ((JoinProcessor) process3);
    assertEquals(message, List.of("product_id"), mprocess3.getForeignKeyNames());
    assertEquals(message, 2, mprocess3.getActions().size());
    assertEquals(message, "inventory_context", mprocess3.getActions().get(0).getContext());
    assertEquals(message, "inventory", mprocess3.getActions().get(0).getAttribute());
    assertNull(message, mprocess3.getActions().get(0).getAs());
    assertEquals(message, "inventory_context", mprocess3.getActions().get(1).getContext());
    assertEquals(message, "warehouse_city_id", mprocess3.getActions().get(1).getAttribute());
    assertNull(message, mprocess3.getActions().get(1).getAs());

    assertEquals(
        stages.get(4).toString(), "brand_context.[product_brand_id]", stages.get(4).getName());
    final Function<IngestState, IngestState> process4 = stages.get(4).getProcess();
    message = process4.toString();
    assertTrue(message, process4 instanceof JoinProcessor);
    final JoinProcessor mprocess4 = ((JoinProcessor) process4);
    assertEquals(message, List.of("product_brand_id"), mprocess4.getForeignKeyNames());
    assertEquals(message, 2, mprocess4.getActions().size());
    assertEquals(message, "brand_context", mprocess4.getActions().get(0).getContext());
    assertEquals(message, "brand_name", mprocess4.getActions().get(0).getAttribute());
    assertNull(message, mprocess4.getActions().get(0).getAs());
    assertEquals(message, "brand_context", mprocess4.getActions().get(1).getContext());
    assertEquals(message, "headquaters_city_id", mprocess4.getActions().get(1).getAttribute());
    assertNull(message, mprocess4.getActions().get(1).getAs());

    assertEquals(
        stages.get(5).toString(), "city_context.[warehouse_city_id]", stages.get(5).getName());
    final Function<IngestState, IngestState> process5 = stages.get(5).getProcess();
    message = process5.toString();
    assertTrue(message, process5 instanceof JoinProcessor);
    final JoinProcessor mprocess5 = ((JoinProcessor) process5);
    assertEquals(message, List.of("warehouse_city_id"), mprocess5.getForeignKeyNames());
    assertEquals(message, 1, mprocess5.getActions().size());
    assertEquals(message, "city_context", mprocess5.getActions().get(0).getContext());
    assertEquals(message, "city_name", mprocess5.getActions().get(0).getAttribute());
    assertEquals(message, "warehouse_city", mprocess5.getActions().get(0).getAs());

    // test exceeding depth limitation
    try {
      StreamConfig mySignal = signal.duplicate();
      mySignal.setName("signal_join_headquaters_city");

      final String src =
          "{"
              + "  'name': 'join_headquaters_city',"
              + "  'condition': 'headquaters_city_id',"
              + "  'missingLookupPolicy': 'strict',"
              + "  'actions': ["
              + "    {"
              + "      'actionType': 'MERGE',"
              + "      'context': 'city_context',"
              + "      'attribute': 'city_name',"
              + "      'as': 'headquaters_city_name'"
              + "    }"
              + "  ]"
              + "}";

      mySignal.addPreprocess(objectMapper.readValue(src.replace("'", "\""), PreprocessDesc.class));
      admin.addStream(tenantName, mySignal, RequestPhase.INITIAL);
      fail("exception is epected");
    } catch (ConstraintViolationException e) {
      System.out.println(e);
      assertEquals(
          "Constraint violation: Enrichment chain length exceeds allowed maximum 2;"
              + " tenant=preprocess_validator_test, signal=signal_join_headquaters_city,"
              + " preprocess=join_headquaters_city, attribute=headquaters_city_id",
          e.getMessage());
    }

    // try another depth path
    try {
      StreamConfig mySignal = signal.duplicate();
      mySignal.setName("signal_join_factory_city");

      final String src0 =
          "{"
              + "  'actionType': 'MERGE',"
              + "  'context': 'city_context',"
              + "  'attribute': 'factory_city_id'"
              + "}";

      final ActionDesc action = objectMapper.readValue(src0.replace("'", "\""), ActionDesc.class);
      mySignal.getPreprocesses().get(1).addAction(action);

      final String src =
          "{"
              + "  'name': 'join_factory_city',"
              + "  'condition': 'factory_city_id',"
              + "  'missingLookupPolicy': 'strict',"
              + "  'actions': ["
              + "    {"
              + "      'actionType': 'MERGE',"
              + "      'context': 'city_context',"
              + "      'attribute': 'city_name',"
              + "      'as': 'factory_city_name'"
              + "    }"
              + "  ]"
              + "}";

      mySignal.addPreprocess(objectMapper.readValue(src.replace("'", "\""), PreprocessDesc.class));
      admin.addStream(tenantName, mySignal, RequestPhase.INITIAL);
      fail("exception is epected");
    } catch (ConstraintViolationException e) {
      System.out.println(e);
      assertEquals(
          "Constraint violation: Enrichment chain length exceeds allowed maximum 2;"
              + " tenant=preprocess_validator_test, signal=signal_join_factory_city,"
              + " preprocess=join_factory_city, attribute=factory_city_id",
          e.getMessage());
    }
  }

  @Test
  public void testSplitActions() throws Exception {

    admin.addStream(tenantName, cityContext, RequestPhase.INITIAL);
    admin.addStream(tenantName, cityContext, RequestPhase.FINAL);

    final String signalSrc =
        "{"
            + "  'name': 'split_preprocess_actions',"
            + "  'type': 'signal',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'order_id', 'type': 'int'},"
            + "    {'name': 'city_id', 'type': 'int'}"
            + "  ],"
            + "  'preprocesses': ["
            + "    {"
            + "      'name': 'city_preprocess_1',"
            + "      'condition': 'city_id',"
            + "      'missingLookupPolicy': 'strict',"
            + "      'actions': ["
            + "        {"
            + "          'actionType': 'MERGE',"
            + "          'context': 'city_context',"
            + "          'attribute': 'city_name',"
            + "          'as': 'city_name'"
            + "        }"
            + "      ]"
            + "    },"
            + "    {"
            + "      'name': 'city_preprocess_2',"
            + "      'condition': 'city_id',"
            + "      'missingLookupPolicy': 'strict',"
            + "      'actions': ["
            + "        {"
            + "          'actionType': 'MERGE',"
            + "          'context': 'city_context',"
            + "          'attribute': 'supplier_city_id',"
            + "          'as': 'supplier_city_id'"
            + "        }"
            + "      ]"
            + "    },"
            + "    {"
            + "      'name': 'city_preprocess_3',"
            + "      'condition': 'supplier_city_id',"
            + "      'missingLookupPolicy': 'strict',"
            + "      'actions': ["
            + "        {"
            + "          'actionType': 'MERGE',"
            + "          'context': 'city_context',"
            + "          'attribute': 'city_name',"
            + "          'as': 'supplier_city_name'"
            + "        }"
            + "      ]"
            + "    }"
            + "  ],"
            + "  'views': [],"
            + "  'postprocesses': []"
            + "}";

    final StreamConfig mySignal =
        objectMapper
            .readValue(signalSrc.replace("'", "\""), StreamConfig.class)
            .setVersion(System.currentTimeMillis());

    admin.addStream(tenantName, mySignal, RequestPhase.INITIAL);
    admin.addStream(tenantName, mySignal, RequestPhase.FINAL);

    StreamDesc registeredSignal = admin.getStream(tenantName, mySignal.getName());
    // System.out.println(registeredSignal);
    final List<ProcessStage> stages = registeredSignal.getPreprocessStages();
    assertNotNull(stages);
    assertEquals(stages.toString(), 2, stages.size());
    assertEquals(stages.get(0).toString(), "city_context.[city_id]", stages.get(0).getName());
    final Function<IngestState, IngestState> process0 = stages.get(0).getProcess();
    String message = process0.toString();
    assertTrue(message, process0 instanceof JoinProcessor);
    final JoinProcessor mprocess0 = ((JoinProcessor) process0);
    assertEquals(message, List.of("city_id"), mprocess0.getForeignKeyNames());
    assertEquals(message, 2, mprocess0.getActions().size());
    assertEquals(message, "city_context", mprocess0.getActions().get(0).getContext());
    assertEquals(message, "city_name", mprocess0.getActions().get(0).getAttribute());
    assertEquals(message, "city_name", mprocess0.getActions().get(0).getAs());
    assertEquals(message, "city_context", mprocess0.getActions().get(1).getContext());
    assertEquals(message, "supplier_city_id", mprocess0.getActions().get(1).getAttribute());
    assertEquals(message, "supplier_city_id", mprocess0.getActions().get(1).getAs());

    final Function<IngestState, IngestState> process1 = stages.get(1).getProcess();
    message = process1.toString();
    assertTrue(message, process1 instanceof JoinProcessor);
    final JoinProcessor mprocess1 = ((JoinProcessor) process1);
    assertEquals(message, List.of("supplier_city_id"), mprocess1.getForeignKeyNames());
    assertEquals(message, 1, mprocess1.getActions().size());
    assertEquals(message, "city_context", mprocess1.getActions().get(0).getContext());
    assertEquals(message, "city_name", mprocess1.getActions().get(0).getAttribute());
    assertEquals(message, "supplier_city_name", mprocess1.getActions().get(0).getAs());
  }

  @Test(expected = ConstraintViolationException.class)
  public void testSplitActionsWrongOrder() throws Exception {

    admin.addStream(tenantName, cityContext, RequestPhase.INITIAL);
    admin.addStream(tenantName, cityContext, RequestPhase.FINAL);

    final List<PreprocessDesc> preprocesses = signal.getPreprocesses();
    final PreprocessDesc temp = preprocesses.get(0);
    preprocesses.set(0, preprocesses.get(1));
    preprocesses.set(1, temp);
    admin.addStream(tenantName, signal, RequestPhase.INITIAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testMlpMissing() throws Exception {

    admin.addStream(tenantName, cityContext, RequestPhase.INITIAL);
    admin.addStream(tenantName, cityContext, RequestPhase.FINAL);

    signal.getPreprocesses().get(0).setMissingLookupPolicy(null);

    admin.addStream(tenantName, signal, RequestPhase.INITIAL);
  }

  // Regression BB-719 preprocess with empty actions should be rejected.
  @Test(expected = ConstraintViolationException.class)
  public void testEmptyEnrichment() throws Exception {
    final String src =
        "{"
            + " 'name': 'Sig0302221916',"
            + " 'type': 'signal',"
            + " 'missingValuePolicy': 'strict',"
            + " 'attributes': [{'name': 'user_id', 'type': 'int'},"
            + "                {'name': 'cc_number', 'type': 'string'},"
            + "                {'name': 'date', 'type': 'string'},"
            + "                {'name': 'city', 'type': 'string'},"
            + "                {'name': 'state', 'type': 'string'},"
            + "                {'name': 'transaction_id', 'type': 'string'}],"
            + " 'preprocesses': [{'actions': [],"
            + "                   'condition': 'user_id',"
            + "                   'missingLookupPolicy': 'strict',"
            + "                   'name': 'Enrich1'}],"
            + " 'views': [{'attributes': ['user_id'],"
            + "            'groupBy': ['cc_number', 'date', 'city', 'state', 'transaction_id'],"
            + "            'name': 'Ftr0302221916',"
            + "            'schemaVersion': 1583205683936}],"
            + " 'postprocesses': [{'rollups': [{'horizon': {'timeunit': 'minute', 'value': 5},"
            + "                                 'interval': {'timeunit': 'minute', 'value': 5},"
            + "                                 'name': 'rlx_Ftr0302221916',"
            + "                                 'schemaName': 'rlx_Ftr0302221916'}],"
            + "                    'view': 'Ftr0302221916'}]"
            + "}";

    final StreamConfig conf = objectMapper.readValue(src.replace("'", "\""), StreamConfig.class);
    conf.setVersion(System.currentTimeMillis());
    admin.addStream(tenantName, conf, RequestPhase.INITIAL);
    admin.addStream(tenantName, conf, RequestPhase.FINAL);
  }

  @Test
  public void testMultiDimensionFundamental() throws Exception {
    admin.addStream(tenantName, counterContext, RequestPhase.INITIAL);
    admin.addStream(tenantName, counterContext, RequestPhase.FINAL);

    admin.addStream(tenantName, orderRecord, RequestPhase.INITIAL);
    admin.addStream(tenantName, orderRecord, RequestPhase.FINAL);

    StreamDesc retrieved = admin.getStream(tenantName, orderRecord.getName());

    verifyValidEnrichmentByProduct(retrieved);
  }

  @Test
  public void testMultiDimensionDifferentForeignKeyCases() throws Exception {
    admin.addStream(tenantName, counterContext, RequestPhase.INITIAL);
    admin.addStream(tenantName, counterContext, RequestPhase.FINAL);

    orderRecord
        .getPreprocesses()
        .get(0)
        .setForeignKey(List.of("fulfillmentcentertype", "fulfillmentcenterid", "productid"));

    admin.addStream(tenantName, orderRecord, RequestPhase.INITIAL);
    admin.addStream(tenantName, orderRecord, RequestPhase.FINAL);

    StreamDesc retrieved = admin.getStream(tenantName, orderRecord.getName());

    verifyValidEnrichmentByProduct(retrieved);
  }

  @Test
  public void testMultiDimensionDifferentEnrichmentAttributeCases() throws Exception {
    admin.addStream(tenantName, counterContext, RequestPhase.INITIAL);
    admin.addStream(tenantName, counterContext, RequestPhase.FINAL);

    orderRecord.getPreprocesses().get(0).getActions().get(0).setAttribute("onhandvalue");
    orderRecord.getPreprocesses().get(0).getActions().get(1).setAttribute("demandvalue");

    admin.addStream(tenantName, orderRecord, RequestPhase.INITIAL);
    admin.addStream(tenantName, orderRecord, RequestPhase.FINAL);

    StreamDesc retrieved = admin.getStream(tenantName, orderRecord.getName());

    verifyValidEnrichmentByProduct(retrieved);
  }

  @Test
  public void testNoEnrichmentName() throws Exception {
    admin.addStream(tenantName, counterContext, RequestPhase.INITIAL);
    admin.addStream(tenantName, counterContext, RequestPhase.FINAL);

    orderRecord.getPreprocesses().get(0).setName(null);

    assertThrows(
        ConstraintViolationException.class,
        () -> admin.addStream(tenantName, orderRecord, RequestPhase.INITIAL));
  }

  @Test
  public void testNoRemoteContext() throws Exception {
    admin.addStream(tenantName, counterContext, RequestPhase.INITIAL);
    admin.addStream(tenantName, counterContext, RequestPhase.FINAL);

    orderRecord.getPreprocesses().get(0).getActions().get(0).setContext(null);

    assertThrows(
        ConstraintViolationException.class,
        () -> admin.addStream(tenantName, orderRecord, RequestPhase.INITIAL));
  }

  @Test
  public void testWrongRemoteContext() throws Exception {
    admin.addStream(tenantName, counterContext, RequestPhase.INITIAL);
    admin.addStream(tenantName, counterContext, RequestPhase.FINAL);

    orderRecord.getPreprocesses().get(0).getActions().get(0).setContext("wrong");

    assertThrows(
        ConstraintViolationException.class,
        () -> admin.addStream(tenantName, orderRecord, RequestPhase.INITIAL));
  }

  @Test
  public void testReferringSignal() throws Exception {
    admin.addStream(tenantName, cityContext, RequestPhase.INITIAL);
    admin.addStream(tenantName, cityContext, RequestPhase.FINAL);
    admin.addStream(tenantName, signal, RequestPhase.INITIAL);
    admin.addStream(tenantName, signal, RequestPhase.FINAL);

    admin.addStream(tenantName, counterContext, RequestPhase.INITIAL);
    admin.addStream(tenantName, counterContext, RequestPhase.FINAL);

    orderRecord.getPreprocesses().get(0).getActions().get(0).setContext(signal.getName());

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> admin.addStream(tenantName, orderRecord, RequestPhase.INITIAL));
    assertThat(
        exception.getMessage(),
        startsWith("Constraint violation: Remote context is of wrong type of stream (signal)"));
  }

  @Test
  public void testBlankEmptyEnrichmentName() throws Exception {
    admin.addStream(tenantName, counterContext, RequestPhase.INITIAL);
    admin.addStream(tenantName, counterContext, RequestPhase.FINAL);

    orderRecord.getPreprocesses().get(0).setName(" ");

    assertThrows(
        ConstraintViolationException.class,
        () -> admin.addStream(tenantName, orderRecord, RequestPhase.INITIAL));
  }

  @Test
  public void testEnrichmentNameConflict() throws Exception {
    admin.addStream(tenantName, cityContext, RequestPhase.INITIAL);
    admin.addStream(tenantName, cityContext, RequestPhase.FINAL);

    signal.getPreprocesses().get(1).setName("city_preprocess_1");

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> admin.addStream(tenantName, signal, RequestPhase.INITIAL));
    assertThat(
        exception.getMessage(),
        startsWith("Constraint violation: Preprocess name may not duplicate;"));
  }

  @Test
  public void testNoForeignKey() throws Exception {
    admin.addStream(tenantName, counterContext, RequestPhase.INITIAL);
    admin.addStream(tenantName, counterContext, RequestPhase.FINAL);

    orderRecord.getPreprocesses().get(0).setForeignKey(null);

    assertThrows(
        ConstraintViolationException.class,
        () -> admin.addStream(tenantName, orderRecord, RequestPhase.INITIAL));
  }

  @Test
  public void testEmptyForeignKey() throws Exception {
    admin.addStream(tenantName, counterContext, RequestPhase.INITIAL);
    admin.addStream(tenantName, counterContext, RequestPhase.FINAL);

    orderRecord.getPreprocesses().get(0).setForeignKey(List.of());

    assertThrows(
        ConstraintViolationException.class,
        () -> admin.addStream(tenantName, orderRecord, RequestPhase.INITIAL));
  }

  @Test
  public void testLessForeignKey() throws Exception {
    admin.addStream(tenantName, counterContext, RequestPhase.INITIAL);
    admin.addStream(tenantName, counterContext, RequestPhase.FINAL);

    orderRecord.getPreprocesses().get(0).setForeignKey(List.of("fulfillmentCenterId", "productId"));

    assertThrows(
        ConstraintViolationException.class,
        () -> admin.addStream(tenantName, orderRecord, RequestPhase.INITIAL));
  }

  @Test
  public void testMoreForeignKey() throws Exception {
    admin.addStream(tenantName, counterContext, RequestPhase.INITIAL);
    admin.addStream(tenantName, counterContext, RequestPhase.FINAL);

    orderRecord
        .getPreprocesses()
        .get(0)
        .setForeignKey(
            List.of(
                "fulfillmentCenterType", "fulfillmentCenterId", "productId", "productCategory"));

    assertThrows(
        ConstraintViolationException.class,
        () -> admin.addStream(tenantName, orderRecord, RequestPhase.INITIAL));
  }

  @Test
  public void testForeignKeyTypeMismatch() throws Exception {
    admin.addStream(tenantName, counterContext, RequestPhase.INITIAL);
    admin.addStream(tenantName, counterContext, RequestPhase.FINAL);

    orderRecord
        .getPreprocesses()
        .get(0)
        .setForeignKey(List.of("fulfillmentCenterId", "fulfillmentCenterType", "productId"));

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> admin.addStream(tenantName, orderRecord, RequestPhase.INITIAL));
    assertThat(
        exception.getMessage(),
        startsWith(
            "Constraint violation:"
                + " Type of foreignKey[0] must match the type of the context's primary key;"));
  }

  @Test
  public void testForeignKeyIncludesEmptyString() throws Exception {
    admin.addStream(tenantName, counterContext, RequestPhase.INITIAL);
    admin.addStream(tenantName, counterContext, RequestPhase.FINAL);

    orderRecord
        .getPreprocesses()
        .get(0)
        .setForeignKey(Arrays.asList("fulfillmentCenterId", "", "productId"));

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> admin.addStream(tenantName, orderRecord, RequestPhase.INITIAL));
    assertThat(
        exception.getMessage(), startsWith("Constraint violation: foreignKey[1] is not set;"));
  }

  @Test
  public void testForeignKeyNotInAttributes() throws Exception {
    admin.addStream(tenantName, counterContext, RequestPhase.INITIAL);
    admin.addStream(tenantName, counterContext, RequestPhase.FINAL);

    orderRecord
        .getPreprocesses()
        .get(0)
        .setForeignKey(Arrays.asList("fulfillmentCenterId", "noSuchAttribute", "productId"));

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> admin.addStream(tenantName, orderRecord, RequestPhase.INITIAL));
    assertThat(
        exception.getMessage(),
        startsWith(
            "Constraint violation: Foreign key attribute must be one of the signal attributes;"));
  }

  @Test
  public void testForeignKeyHasEmptyDefaultValue() throws Exception {
    admin.addStream(tenantName, counterContext, RequestPhase.INITIAL);
    admin.addStream(tenantName, counterContext, RequestPhase.FINAL);

    orderRecord.getAttributes().get(1).setMissingValuePolicy(MissingAttributePolicyV1.USE_DEFAULT);
    orderRecord.getAttributes().get(1).setDefaultValue("");

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> admin.addStream(tenantName, orderRecord, RequestPhase.INITIAL));
    assertThat(
        exception.getMessage(),
        startsWith(
            "Constraint violation: Default value of string type foreignKey[0] may not be empty;"));
  }

  @Test
  public void testEnrichmentAttributeMissing() throws Exception {
    admin.addStream(tenantName, counterContext, RequestPhase.INITIAL);
    admin.addStream(tenantName, counterContext, RequestPhase.FINAL);

    orderRecord.getPreprocesses().get(0).getActions().get(0).setAttribute(null);

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> admin.addStream(tenantName, orderRecord, RequestPhase.INITIAL));
    assertThat(
        exception.getMessage(),
        startsWith("Constraint violation: Enrichment attribute name may not be empty;"));
  }

  @Test
  public void testEnrichmentAttributeDoesNotExist() throws Exception {
    admin.addStream(tenantName, counterContext, RequestPhase.INITIAL);
    admin.addStream(tenantName, counterContext, RequestPhase.FINAL);

    orderRecord.getPreprocesses().get(0).getActions().get(0).setAttribute("productCategory");

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> admin.addStream(tenantName, orderRecord, RequestPhase.INITIAL));
    assertThat(
        exception.getMessage(),
        startsWith(
            "Constraint violation:"
                + " Enriching attribute name must exist in referring context;"));
  }

  @Test
  public void testEnrichmentAttributeDefaultMissing() throws Exception {
    admin.addStream(tenantName, counterContext, RequestPhase.INITIAL);
    admin.addStream(tenantName, counterContext, RequestPhase.FINAL);

    orderRecord
        .getPreprocesses()
        .get(0)
        .setMissingLookupPolicy(MissingAttributePolicyV1.USE_DEFAULT);

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> admin.addStream(tenantName, orderRecord, RequestPhase.INITIAL));
    assertThat(
        exception.getMessage(),
        startsWith(
            "Constraint violation: Enrichment attribute with missingLookupPolicy"
                + " 'StoreFillInValue' must have 'default' property;"));
  }

  @Test
  public void testEnrichedAttributeConflictsWithSignalAttribute() throws Exception {
    admin.addStream(tenantName, counterContext, RequestPhase.INITIAL);
    admin.addStream(tenantName, counterContext, RequestPhase.FINAL);

    orderRecord.getPreprocesses().get(0).getActions().get(0).setAs("productCategory");

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> admin.addStream(tenantName, orderRecord, RequestPhase.INITIAL));
    assertThat(
        exception.getMessage(),
        startsWith(
            "Constraint violation:"
                + " Enriched attribute name must not conflict with other attributes;"));
  }

  void verifyValidEnrichmentByProduct(StreamDesc signalDesc) {
    final List<ProcessStage> stages = signalDesc.getPreprocessStages();
    assertNotNull(stages);
    assertEquals(stages.toString(), 1, stages.size());
    assertEquals(
        stages.get(0).toString(),
        "inventorycounter.[fulfillmentcentertype, fulfillmentcenterid, productid]",
        stages.get(0).getName());
    final Function<IngestState, IngestState> process0 = stages.get(0).getProcess();
    String message = process0.toString();
    assertTrue(message, process0 instanceof JoinProcessor);
    final JoinProcessor mprocess0 = ((JoinProcessor) process0);
    assertEquals(
        message,
        List.of("fulfillmentCenterType", "fulfillmentCenterId", "productId"),
        mprocess0.getForeignKeyNames());
    assertEquals(message, 2, mprocess0.getActions().size());
    assertEquals(message, "inventoryCounter", mprocess0.getActions().get(0).getContext());
    assertEquals(message, "onHandValue", mprocess0.getActions().get(0).getAttribute());
    assertEquals(message, "onHand", mprocess0.getActions().get(0).getAs());
    assertEquals(message, "inventoryCounter", mprocess0.getActions().get(1).getContext());
    assertEquals(message, "demandValue", mprocess0.getActions().get(1).getAttribute());
    assertEquals(message, "demand", mprocess0.getActions().get(1).getAs());
  }
}
