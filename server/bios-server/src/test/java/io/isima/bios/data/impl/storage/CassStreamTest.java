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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.isima.bios.admin.StreamConfigTranslator;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.errors.exception.InvalidFilterException;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.exceptions.validator.ConstraintViolationValidatorException;
import io.isima.bios.exceptions.validator.NotImplementedValidatorException;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.BeforeClass;
import org.junit.Test;

public class CassStreamTest {

  private static CassTenant dummyTenant;
  private static Long timestamp;

  @BeforeClass
  public static void setUpClass() {
    dummyTenant = new CassTenant(new TenantDesc("dummy", System.currentTimeMillis(), false));
    timestamp = 155058480542L;
  }

  @Test
  public void constructWithSignalStream() {
    StreamDesc streamDesc = new StreamDesc("signal_stream", timestamp);
    streamDesc.setType(StreamType.SIGNAL);
    CassStream cassStreamConfig = CassStream.create(dummyTenant, streamDesc, null);
    assertEquals("evt_0fcf52fb2de7392689c55582063fbe63", cassStreamConfig.getTableName());
  }

  @Test
  public void constructWithContextStream() {
    StreamDesc streamDesc = new StreamDesc("context_stream", timestamp);
    streamDesc.setType(StreamType.CONTEXT);
    streamDesc.addAttribute(new AttributeDesc("one", InternalAttributeType.STRING));
    streamDesc.addAttribute(new AttributeDesc("two", InternalAttributeType.LONG));
    streamDesc.setPrimaryKey(List.of("one"));
    CassStream cassStreamConfig = CassStream.create(dummyTenant, streamDesc, null);
    assertEquals("cx2_07b581cd43e2375eb566790fa1c405fd", cassStreamConfig.getTableName());
  }

  @Test
  public void valueTypeInterpreter() {
    for (InternalAttributeType type : InternalAttributeType.values()) {
      assertNotNull(type.name(), CassStream.valueTypeToCassDataTypeName(type));
    }
  }

  @Test
  public void testGenerateCreateTableStatement() {
    StreamDesc streamDesc = new StreamDesc("signal_stream", timestamp);
    streamDesc.setType(StreamType.SIGNAL);
    List<AttributeDesc> attributeDescs = new ArrayList<>();
    attributeDescs.add(new AttributeDesc("one", InternalAttributeType.STRING));
    attributeDescs.add(new AttributeDesc("two", InternalAttributeType.UUID));
    streamDesc.setAttributes(attributeDescs);
    streamDesc.setPrimaryKey(List.of("one"));
    Long streamVersion = 10101111010L;
    streamDesc.setVersion(streamVersion);

    CassStream cassStreamConfig = CassStream.create(dummyTenant, streamDesc, null);
    String statement = cassStreamConfig.makeCreateTableStatement("evt_mytenant");

    assertEquals(
        String.format(
            "CREATE TABLE IF NOT EXISTS evt_mytenant.evt_c1060fb7795e3599be012d5bcd83cd9f"
                + " (time_index timestamp, event_id timeuuid, evt_one text, evt_two uuid,"
                + " PRIMARY KEY (time_index, event_id)) WITH CLUSTERING ORDER BY (event_id ASC)"
                + " AND comment = 'tenant=dummy (version=%d),"
                + " signal=signal_stream (version=%d, schemaVersion=%d)'"
                + " AND default_time_to_live = 5184000"
                + " AND gc_grace_seconds = 259200"
                + " AND compaction ="
                + " {'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy',"
                + " 'compaction_window_size': '6', 'compaction_window_unit': 'HOURS',"
                + " 'tombstone_compaction_interval': '43200', 'tombstone_threshold': '0.8',"
                + " 'unchecked_tombstone_compaction': 'true'}"
                + " AND bloom_filter_fp_chance = 0.01",
            dummyTenant.getVersion(), streamVersion, streamVersion),
        statement);

    streamDesc.setType(StreamType.CONTEXT);
    cassStreamConfig = CassStream.create(dummyTenant, streamDesc, null);
    statement = cassStreamConfig.makeCreateTableStatement("evt_mytenant");
    System.out.println(statement);
  }

  @Test
  public void testGenerateCreateTableStatementWithCompaction()
      throws JsonProcessingException,
          ConstraintViolationValidatorException,
          NotImplementedValidatorException {
    final var src =
        "{"
            + "  'signalName': 'inventoryCounterUpdates',"
            + "  'missingAttributePolicy': 'Reject',"
            + "  'attributes': ["
            + "    {"
            + "      'attributeName': 'fcType',"
            + "      'type': 'String'"
            + "    },"
            + "    {"
            + "      'attributeName': 'fcId',"
            + "      'type': 'Integer'"
            + "    },"
            + "    {"
            + "      'attributeName': 'itemId',"
            + "      'type': 'Integer'"
            + "    },"
            + "    {"
            + "      'attributeName': 'operation',"
            + "      'type': 'String',"
            + "      'allowedValues': ['delta', 'set']"
            + "    },"
            + "    {"
            + "      'attributeName': 'onHandValue',"
            + "      'type': 'Decimal'"
            + "    },"
            + "    {"
            + "      'attributeName': 'demandValue',"
            + "      'type': 'Decimal'"
            + "    }"
            + "  ],"
            + "  'postStorageStage': {"
            + "    'features': ["
            + "      {"
            + "        'featureName': 'byAll',"
            + "        'dimensions': ['fcType', 'fcId', 'itemId'],"
            + "        'attributes': ['onHandValue', 'demandValue'],"
            + "        'featureInterval': 100,"
            + "        'materializedAs': 'AccumulatingCount',"
            + "        'featureAsContextName': 'inventoryCounterSnapshots'"
            + "      }"
            + "    ]"
            + "  }"
            + "}";
    final var mapper = BiosObjectMapperProvider.get();

    final var signalConfig = mapper.readValue(src.replace("'", "\""), SignalConfig.class);
    final var streamConfig = StreamConfigTranslator.toTfos(signalConfig);
    final var signalDesc = new StreamDesc(streamConfig);
    Long streamVersion = 10101111010L;
    signalDesc.setVersion(streamVersion);
    signalDesc.setSchemaVersion(streamVersion);

    CassStream cassStreamConfig = CassStream.create(dummyTenant, signalDesc, null);
    String statement = cassStreamConfig.makeCreateTableStatement("evt_mytenant");

    System.out.println(statement);

    assertEquals(
        String.format(
            "CREATE TABLE IF NOT EXISTS evt_mytenant.evt_0aa296d5a9843ebc8627f38db28ed4c7"
                + " (time_index timestamp, event_id timeuuid, evt_fctype text, evt_fcid bigint,"
                + " evt_itemid bigint, evt_operation int,"
                + " evt_onhandvalue double, evt_demandvalue double,"
                + " PRIMARY KEY (time_index, event_id)) WITH CLUSTERING ORDER BY (event_id ASC)"
                + " AND comment = 'tenant=dummy (version=%d),"
                + " signal=inventoryCounterUpdates (version=%d, schemaVersion=%d)'"
                + " AND default_time_to_live = 5184000"
                + " AND gc_grace_seconds = 259200"
                + " AND compaction ="
                + " {'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy',"
                + " 'compaction_window_size': '6', 'compaction_window_unit': 'HOURS',"
                + " 'tombstone_compaction_interval': '43200', 'tombstone_threshold': '0.8',"
                + " 'unchecked_tombstone_compaction': 'true'}"
                + " AND bloom_filter_fp_chance = 0.01",
            dummyTenant.getVersion(), streamVersion, streamVersion),
        statement);
  }

  @Test
  public void testGenerateIngestPreparedStatement()
      throws NoSuchTenantException, NoSuchStreamException {

    final StreamDesc streamDesc = new StreamDesc("signal_stream", timestamp);
    streamDesc.setType(StreamType.SIGNAL);
    final List<AttributeDesc> attributeDescs = new ArrayList<>();
    attributeDescs.add(new AttributeDesc("one", InternalAttributeType.STRING));
    attributeDescs.add(new AttributeDesc("two", InternalAttributeType.UUID));
    streamDesc.setAttributes(attributeDescs);

    final CassStream cassStream = CassStream.create(dummyTenant, streamDesc, null);

    final String statement = cassStream.generateIngestQueryString("evt_mytenant");

    assertEquals(
        "INSERT INTO evt_mytenant.evt_0fcf52fb2de7392689c55582063fbe63 "
            + "(time_index, event_id, evt_one, evt_two) VALUES (?, ?, ?, ?)",
        statement);
  }

  /**
   * Test CassStream.populateIngestValues method.
   *
   * @throws ApplicationException
   */
  @Test
  public void testPopulateIngestValues() throws ApplicationException {
    final List<AttributeDesc> attributeDescs = new ArrayList<>();
    attributeDescs.add(new AttributeDesc("one", InternalAttributeType.STRING));
    attributeDescs.add(new AttributeDesc("two", InternalAttributeType.UUID));
    attributeDescs.add(new AttributeDesc("three", InternalAttributeType.INT));

    final List<AttributeDesc> optionalAttrs = new ArrayList<>();
    optionalAttrs.add(new AttributeDesc("four", InternalAttributeType.INT));
    optionalAttrs.add(new AttributeDesc("five", InternalAttributeType.STRING));

    // we'll fill two dummy values first, then two more
    final Object[] values = new Object[2 + attributeDescs.size() + 2];
    int index = 0;
    values[index++] = "aabbcc";
    values[index++] = 980;

    final Event event = new EventJson();
    final Map<String, Object> attributes = event.getAttributes();
    attributes.put("five", "55555");
    attributes.put("two", UUID.fromString("b2779989-2c1c-44b2-963a-dcafb2e2f19c"));
    attributes.put("three", 888);
    attributes.put("one", "11111");

    final int newIndex = CassStream.populateIngestValues(index, event, attributeDescs, values);
    assertEquals(5, newIndex);
    final int finalIndex = CassStream.populateIngestValues(newIndex, event, optionalAttrs, values);
    assertEquals(7, finalIndex);

    assertEquals("aabbcc", values[0]);
    assertEquals(980, values[1]);
    assertEquals("11111", values[2]);
    assertEquals(UUID.fromString("b2779989-2c1c-44b2-963a-dcafb2e2f19c"), values[3]);
    assertEquals(888, values[4]);
    assertEquals(null, values[5]);
    assertEquals("55555", values[6]);
  }

  @Test
  public void getFilterStringRawValueTest() throws Exception {
    final var stringAttribute = new AttributeDesc("stringAttribute", InternalAttributeType.STRING);
    assertThat(
        CassStream.getFilterRawValue("'hello world'", null, stringAttribute), is("hello world"));

    final var todaysWeather = "'My father\\'s favorite in Trader Joe\\'s'";
    System.out.println(todaysWeather);
    assertThat(
        CassStream.getFilterRawValue(todaysWeather, null, stringAttribute),
        is("My father's favorite in Trader Joe's"));

    assertThrows(
        InvalidFilterException.class,
        () -> CassStream.getFilterRawValue("no quotes", null, stringAttribute));

    assertThrows(
        InvalidFilterException.class,
        () -> CassStream.getFilterRawValue("'quotes not closing", null, stringAttribute));

    assertThrows(
        InvalidFilterException.class,
        () -> CassStream.getFilterRawValue("quotes not opening'", null, stringAttribute));
  }
}
