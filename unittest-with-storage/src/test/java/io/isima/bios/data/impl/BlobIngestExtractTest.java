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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.data.DataEngine;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.InvalidFilterException;
import io.isima.bios.errors.exception.InvalidValueException;
import io.isima.bios.errors.exception.InvalidValueSyntaxException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.it.tools.TestUtils;
import io.isima.bios.models.Count;
import io.isima.bios.models.ExtractRequest;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.Sort;
import io.isima.bios.models.Sum;
import io.isima.bios.models.View;
import io.isima.bios.models.v1.Aggregate;
import io.isima.bios.models.v1.Group;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.server.handlers.DataServiceHandler;
import io.isima.bios.service.HttpClientManager;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class BlobIngestExtractTest {

  private static long timestamp;
  private static AdminInternal admin;
  private static DataEngine dataEngine;
  private static HttpClientManager clientManager;
  private static DataServiceHandler dataServiceHandler;

  private static String src;
  private static String tenantName;
  private static TenantConfig tenantConfig;
  private static String signalName;
  private static String contextName;

  // dummy signatures
  private static byte[] signature1;
  private static byte[] signature2;
  private static byte[] signature3;
  private static byte[] signature4;

  // dummy photo data
  private static byte[] photo1;
  private static byte[] photo2;
  private static byte[] photo3;
  private static byte[] photo4;

  // Async process executor
  ExecutorService service = Executors.newSingleThreadExecutor();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    signature1 = new byte[16];
    signature2 = new byte[16];
    signature3 = new byte[16];
    signature4 = new byte[16];
    Random random = new Random();
    random.nextBytes(signature1);
    random.nextBytes(signature2);
    random.nextBytes(signature3);
    random.nextBytes(signature4);
    signature1[0] = 1;
    signature2[0] = 2;
    signature3[0] = 3;
    signature4[0] = 4;

    photo1 = new byte[32];
    photo2 = new byte[32];
    photo3 = new byte[32];
    photo4 = new byte[32];
    random.nextBytes(photo1);
    random.nextBytes(photo2);
    random.nextBytes(photo3);
    random.nextBytes(photo4);

    Bios2TestModules.startModules(false, BlobIngestExtractTest.class, Map.of());
    admin = BiosModules.getAdminInternal();
    dataEngine = BiosModules.getDataEngine();
    clientManager = BiosModules.getHttpClientManager();
    dataServiceHandler = BiosModules.getDataServiceHandler();

    src =
        "{"
            + "    'name': 'blobIngestExtractTest',"
            + "    'streams': [{"
            + "        'name': 'registration',"
            + "        'type': 'signal',"
            + "        'missingValuePolicy': 'strict',"
            + "        'attributes': ["
            + "            {'name': 'first_name', 'type': 'string'},"
            + "            {'name': 'last_name', 'type': 'string'},"
            + "            {'name': 'signature', 'type': 'blob'},"
            + "            {'name': 'list_value', 'type': 'blob'}"
            + "        ],"
            + "        'preprocesses': [{"
            + "            'name': 'merge_personal_info',"
            + "            'condition': 'signature',"
            + "            'missingLookupPolicy': 'use_default',"
            + "            'actions': ["
            + "                {'actionType': 'merge', 'context': 'personal_info',"
            + "                  'attribute': 'phone', 'defaultValue': 'n/a'},"
            + "                {'actionType': 'merge', 'context': 'personal_info',"
            + "                 'attribute': 'photo', 'defaultValue': '"
            + base64(photo4)
            + "'}"
            + "            ]"
            + "        }]"
            + "    }, {"
            + "        'name': 'personal_info',"
            + "        'type': 'context',"
            + "        'missingValuePolicy': 'strict',"
            + "        'attributes': ["
            + "            {'name': 'signature', 'type': 'blob'},"
            + "            {'name': 'phone', 'type': 'string'},"
            + "            {'name': 'photo', 'type': 'blob'}"
            + "        ]"
            + "    }]"
            + "}";
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Bios2TestModules.shutdown();
  }

  private static String base64(byte[] src) {
    return Base64.getEncoder().encodeToString(src);
  }

  @Before
  public void setUp() throws Exception {
    tenantConfig =
        TfosObjectMapperProvider.get().readValue(src.replaceAll("'", "\""), TenantConfig.class);

    tenantName = tenantConfig.getName();
    signalName = tenantConfig.getStreams().get(0).getName();
    contextName = tenantConfig.getStreams().get(1).getName();

    timestamp = System.currentTimeMillis();
  }

  @After
  public void tearDown() throws Exception {
    timestamp = System.currentTimeMillis();
    try {
      admin.removeTenant(tenantConfig, RequestPhase.INITIAL, ++timestamp);
      admin.removeTenant(tenantConfig, RequestPhase.FINAL, timestamp);
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testBasic() throws Throwable {
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
    admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);

    // test putting normal personal info context entries
    {
      final var entries =
          List.of(
              base64(signature2) + ",650-123-1235," + base64(photo2),
              base64(signature3) + ",650-123-1236," + base64(photo3),
              base64(signature1) + ",650-123-1234," + base64(photo1));
      TestUtils.upsertContextEntries(
          dataServiceHandler, clientManager, tenantName, contextName, entries);
    }

    // test getting personal info context entries
    {
      final List<Object> keys =
          Arrays.asList(
              new String[] {
                base64(signature1), base64(signature2), base64(signature3), base64(signature4)
              });
      final var entries =
          TestUtils.getContextEntriesSingleKeys(dataServiceHandler, tenantName, contextName, keys);
      assertEquals(3, entries.size());
      assertEquals("650-123-1234", entries.get(0).getAttributes().get("phone"));
      assertEquals(ByteBuffer.wrap(photo1), entries.get(0).getAttributes().get("photo"));
      assertEquals("650-123-1235", entries.get(1).getAttributes().get("phone"));
      assertEquals(ByteBuffer.wrap(photo2), entries.get(1).getAttributes().get("photo"));
      assertEquals("650-123-1236", entries.get(2).getAttributes().get("phone"));
      assertEquals(ByteBuffer.wrap(photo3), entries.get(2).getAttributes().get("photo"));
    }

    // test putting invalid personal info context entry
    {
      final var entries = List.of(base64(signature4) + ",111-111-1111,!broken!");

      final var exception =
          assertThrows(
              InvalidValueSyntaxException.class,
              () ->
                  TestUtils.upsertContextEntries(
                      dataServiceHandler, clientManager, tenantName, contextName, entries));
      assertThat(
          exception.getMessage(),
          is(
              "Invalid value syntax: Illegal base64 character 21;"
                  + " attribute=photo, type=Blob, value=!broken!"));
    }

    // test putting context entry with empty key
    {
      final var entries = List.of(",111-111-1111," + base64(photo4));

      final var exception =
          assertThrows(
              InvalidValueException.class,
              () ->
                  TestUtils.upsertContextEntries(
                      dataServiceHandler, clientManager, tenantName, contextName, entries));
      assertThat(
          exception.getMessage(),
          is("Invalid value: First primary key element of type Blob may not be empty"));
    }

    // test listing personal info primary keys
    {
      final var entries =
          TestUtils.listContextPrimaryKeys(admin, dataEngine, tenantName, contextName);
      assertEquals(3, entries.size());
      assertEquals(List.of(ByteBuffer.wrap(signature1)), entries.get(0));
      assertEquals(List.of(ByteBuffer.wrap(signature2)), entries.get(1));
      assertEquals(List.of(ByteBuffer.wrap(signature3)), entries.get(2));
    }

    // test getting context entries (negative case)
    {
      final List<Object> keys = Arrays.asList(new String[] {"!not a b64 string!"});
      final var exception =
          assertThrows(
              InvalidValueSyntaxException.class,
              () ->
                  TestUtils.getContextEntriesSingleKeys(
                      dataServiceHandler, tenantName, contextName, keys));
      assertEquals(
          "Invalid value syntax: Illegal base64 character 21;"
              + " attribute=signature, type=Blob, value=!not a b64 string!",
          exception.getMessage());
    }

    StreamConfig signal = admin.getStream(tenantName, signalName);

    long startTime = Long.MAX_VALUE;
    // Ingest events
    {
      final var response =
          TestUtils.insert(
              dataServiceHandler,
              tenantName,
              signalName,
              "first1,last1," + base64(signature1) + "," + base64(signature1));
      startTime = response.getTimeStamp();
    }
    TestUtils.insert(
        dataServiceHandler,
        tenantName,
        signalName,
        "first4,last4," + base64(signature4) + "," + base64(signature4));

    // Try extracting events
    {
      final ExtractRequest extractRequest = new ExtractRequest();
      extractRequest.setStartTime(startTime);
      extractRequest.setEndTime(startTime + 10 * 1000);
      final var result =
          TestUtils.extract(dataServiceHandler, tenantName, signalName, extractRequest);

      assertEquals(2, result.size());
      final Map<String, Object> attrs0 = result.get(0).getAttributes();
      assertEquals("first1", attrs0.get("first_name"));
      assertEquals("last1", attrs0.get("last_name"));
      assertEquals(ByteBuffer.wrap(signature1), attrs0.get("signature"));
      assertEquals(ByteBuffer.wrap(signature1), attrs0.get("list_value"));
      assertEquals("650-123-1234", attrs0.get("phone"));
      assertEquals(ByteBuffer.wrap(photo1), attrs0.get("photo"));

      final Map<String, Object> attr1 = result.get(1).getAttributes();
      assertEquals("first4", attr1.get("first_name"));
      assertEquals("last4", attr1.get("last_name"));
      assertEquals(ByteBuffer.wrap(signature4), attr1.get("signature"));
      assertEquals(ByteBuffer.wrap(signature4), attr1.get("list_value"));
      assertEquals("n/a", attr1.get("phone"));
      assertEquals(ByteBuffer.wrap(photo4), attr1.get("photo"));
    }

    // Try filter
    {
      final ExtractRequest extractRequest = new ExtractRequest();
      extractRequest.setStartTime(startTime);
      extractRequest.setEndTime(startTime + 10 * 1000);
      extractRequest.setFilter("signature = '" + base64(signature1) + "'");
      final var exception =
          assertThrows(
              TfosException.class,
              () -> TestUtils.extract(dataServiceHandler, tenantName, signalName, extractRequest));
      assertThat(exception, instanceOf(InvalidFilterException.class));
      assertThat(exception.getMessage(), is("Invalid filter: Filtering by BLOB is not supported"));
    }

    // Ingest more for view test
    TestUtils.insert(
        dataServiceHandler,
        tenantName,
        signalName,
        "first2,last2," + base64(signature2) + "," + base64(signature1));

    // Try extracting with group view
    {
      final ExtractRequest extractRequest = new ExtractRequest();
      extractRequest.setStartTime(startTime);
      extractRequest.setEndTime(startTime + 30 * 1000);
      final View view = new Group("list_value");
      extractRequest.setView(view);
      extractRequest.setAggregates(List.of(new Count()));
      extractRequest.setAttributes(List.of("list_value"));
      final var result =
          TestUtils.extract(dataServiceHandler, tenantName, signalName, extractRequest);
      assertEquals(2, result.size());
      // System.out.println(result);
      assertEquals(ByteBuffer.wrap(signature1), result.get(0).getAttributes().get("list_value"));
      assertEquals(2L, result.get(0).getAttributes().get("count()"));
      assertEquals(ByteBuffer.wrap(signature4), result.get(1).getAttributes().get("list_value"));
      assertEquals(1L, result.get(1).getAttributes().get("count()"));
    }

    // Try extracting with sort view
    {
      final ExtractRequest extractRequest = new ExtractRequest();
      extractRequest.setStartTime(startTime);
      extractRequest.setEndTime(startTime + 30 * 1000);
      final View view = new Sort("list_value");
      extractRequest.setView(view);
      extractRequest.setAttributes(List.of("first_name", "last_name", "signature", "list_value"));
      final var result =
          TestUtils.extract(dataServiceHandler, tenantName, signalName, extractRequest);
      assertEquals(3, result.size());
      // System.out.println(result);
      assertEquals("first1", result.get(0).getAttributes().get("first_name"));
      assertEquals("first2", result.get(1).getAttributes().get("first_name"));
      assertEquals("first4", result.get(2).getAttributes().get("first_name"));
    }

    // sum of blob attribute values should be rejected
    {
      final ExtractRequest extractRequest = new ExtractRequest();
      extractRequest.setStartTime(startTime);
      extractRequest.setEndTime(startTime + 30 * 1000);
      final View view = new Group("list_value");
      extractRequest.setView(view);
      final Aggregate sum = new Sum("list_value");
      extractRequest.setAggregates(List.of(sum));
      extractRequest.setAttributes(List.of("list_value"));
      final var exception =
          assertThrows(
              TfosException.class,
              () -> TestUtils.extract(dataServiceHandler, tenantName, signalName, extractRequest));
      assertThat(exception, instanceOf(ConstraintViolationException.class));
      assertThat(
          exception.getMessage(),
          is(
              "Constraint violation: Query #0: Attribute 'list_value' of type BLOB"
                  + " for SUM metric is not addable"));
    }
  }
}
