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
package io.isima.bios.export;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.junit.MatcherAssert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.admin.Translators;
import io.isima.bios.data.synthesis.generators.AttributeGeneratorsCreator;
import io.isima.bios.data.synthesis.generators.RecordGenerator;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.exceptions.validator.ValidatorException;
import io.isima.bios.models.Event;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SignalTransformerTest {
  private static ObjectMapper objectMapper;

  private SignalConfig simpleConfig;

  @BeforeClass
  public static void setUpBeforeClass() {
    objectMapper = BiosObjectMapperProvider.get();
  }

  @Before
  public void setUp() throws Exception {
    final String srcSimple =
        "{"
            + "  'signalName': 'signalExample',"
            + "  'version': 1588175368661,"
            + "  'biosVersion': 1588175368660,"
            + "  'missingAttributePolicy': 'Reject',"
            + "  'attributes': ["
            + "    {"
            + "      'attributeName': 'userName',"
            + "      'type': 'String'"
            + "    },"
            + "    {"
            + "      'attributeName': 'userSalary',"
            + "      'type': 'Decimal'"
            + "    },"
            + "    {"
            + "      'attributeName': 'aBool',"
            + "      'type': 'Boolean'"
            + "    },"
            + "    {"
            + "      'attributeName': 'abc',"
            + "      'type': 'Integer',"
            + "      'category': 'Additive',"
            + "      'missingAttributePolicy': 'StoreDefaultValue',"
            + "      'default': -1,"
            + "      'unit': '$',"
            + "      'unitPosition': 'Prefix',"
            + "      'positiveIndicator': 'High'"
            + "    }"
            + "  ]"
            + "}";

    simpleConfig = objectMapper.readValue(srcSimple.replace("'", "\""), SignalConfig.class);
  }

  @Test
  public void testSimpleArrowConversion()
      throws ValidatorException, TfosException, ApplicationException {
    final var creator = AttributeGeneratorsCreator.getCreator("tenant1", simpleConfig);
    final var generator = new RecordGenerator(creator);
    final var events = buildEvents(generator, 100);

    final StreamConfig stream = Translators.toTfos(simpleConfig);
    Assert.assertEquals("signalExample", stream.getName());

    SignalToArrowTransformer tr = new SignalToArrowTransformer(stream);
    final var start = System.nanoTime();
    tr.transformEvents(events);
    System.out.println(
        "Time elapsed = " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
    final var cntr = new int[1];
    tr.getDataPipe().addBatchConsumer((r) -> cntr[0]++);
    assertThat(cntr[0], is(0));
    tr.getDataPipe().drainPipe();
    assertThat(cntr[0], is(1));
  }

  @Test
  public void testMultiBatchArrowConversion()
      throws ValidatorException, TfosException, ApplicationException {
    final var creator = AttributeGeneratorsCreator.getCreator("tenant1", simpleConfig);
    final var generator = new RecordGenerator(creator);
    final var events1 = buildEvents(generator, 100);
    final var events2 = buildEvents(generator, 67);

    final StreamConfig stream = Translators.toTfos(simpleConfig);
    Assert.assertEquals("signalExample", stream.getName());
    SignalToArrowTransformer tr = new SignalToArrowTransformer(stream);

    tr.transformEvents(events1);
    tr.transformEvents(events2);

    final List<ArrowDataPipe.RecordBatch> records = new ArrayList<>();
    tr.getDataPipe().addBatchConsumer(records::add);
    assertThat(records.size(), is(0));
    tr.getDataPipe().drainPipe();
    assertThat(records.size(), is(2));
  }

  @Test
  public void testArrowParquetConversion()
      throws ValidatorException, TfosException, ApplicationException {
    final var creator = AttributeGeneratorsCreator.getCreator("tenant1", simpleConfig);
    final var generator = new RecordGenerator(creator);

    final StreamConfig stream = Translators.toTfos(simpleConfig);
    Assert.assertEquals("signalExample", stream.getName());
    SignalToArrowTransformer tr = new SignalToArrowTransformer(stream);

    final var parquet =
        new ArrowToParquetTransformer(
            "tenant1", "signalExample", simpleConfig.getVersion(), tr.getDataPipe());
    final int[] counter = new int[3];
    parquet.registerDataSink(
        (b, t) -> {
          counter[0]++;
          counter[1] += b.limit();
          counter[2] += b.position();
        });

    for (int i = 0; i < 10; i++) {

      final var events1 = buildEvents(generator, 2000);
      final var events2 = buildEvents(generator, 2000);
      final var events3 = buildEvents(generator, 20);
      final var events4 = buildEvents(generator, 10);

      tr.transformEvents(events1);
      tr.transformEvents(events2);
      tr.transformEvents(events3);
      tr.transformEvents(events4);

      tr.getDataPipe().drainPipe();
      parquet.flushDataToSink();
      tr.getDataPipe().clearDrain();
      parquet.sinkFlushed();
    }

    assertThat(counter[0], is(10));
    assertThat(counter[1], Matchers.greaterThan(30000));
    assertThat(counter[2], is(0));
  }

  private List<Event> buildEvents(RecordGenerator generator, int cnt) {
    final List<Event> eventList = new ArrayList<>();
    for (int i = 0; i < cnt; i++) {
      eventList.add(generator.generate());
    }
    return eventList;
  }
}
