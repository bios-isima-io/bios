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
package io.isima.bios.query;

import static io.isima.bios.models.isql.Metric.count;
import static io.isima.bios.models.isql.Window.sliding;
import static io.isima.bios.models.isql.Window.tumbling;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.codec.proto.isql.ProtoBuilderProvider;
import io.isima.bios.models.isql.ISqlStatement;
import io.isima.bios.models.isql.QueryValidator;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.util.concurrent.TimeUnit;
import org.hamcrest.junit.ExpectedException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

public class SelectWindowedStatementValidationTest {

  private static ObjectMapper mapper;

  @Rule public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    mapper = TfosObjectMapperProvider.get();
    ProtoBuilderProvider.configureProtoBuilderProvider();
  }

  private StreamConfig signal;
  private QueryValidator validator;

  @Before
  public void setUp() throws Exception {
    final String src =
        "{"
            + "  'name': 'testSignal',"
            + "  'attributes': ["
            + "    {'name': 'county', 'type': 'string'},"
            + "    {'name': 'city', 'type': 'string'},"
            + "    {'name': 'value', 'type': 'long'},"
            + "    {'name': 'latitude', 'type': 'double'},"
            + "    {'name': 'longitude', 'type': 'double'},"
            + "    {'name': 'activated', 'type': 'boolean'},"
            + "    {'name': 'signature', 'type': 'blob'},"
            + "    {'name': 'status', 'type': 'enum',"
            + "     'enum': ['TODO', 'IN_PROGRESS', 'DONE']}"
            + "  ],"
            + "  'views': ["
            + "    {"
            + "      'name': 'byCountryCity',"
            + "      'groupBy': ['country', 'city'],"
            + "      'attributes': ['value', 'latitude', 'longitude']"
            + "    }"
            + "  ],"
            + "  'postprocesses': ["
            + "    {"
            + "      'view': 'byCountryCity',"
            + "      'rollups': ["
            + "        {"
            + "          'name': 'rollupByCountryCity',"
            + "          'interval': {'value': 5, 'timeunit': 'minute'},"
            + "          'horizon': {'value': 5, 'timeunit': 'minute'}"
            + "        }"
            + "      ]"
            + "    }"
            + "  ]"
            + "}";
    signal = mapper.readValue(src.replace("'", "\""), StreamConfig.class);
    validator = new TfosQueryValidator(signal, null);
  }

  @Test
  public void simplestTumblingWindow() throws Exception {
    final var statement =
        ISqlStatement.select(count())
            .from("testSignal")
            .window(tumbling(5, TimeUnit.MINUTES))
            .snappedTimeRange(System.currentTimeMillis(), -3600000)
            .build();
    validator.validate(statement, 0, null);
  }

  @Test
  public void simplestSlidingWindow() throws Exception {
    final var statement =
        ISqlStatement.select(count())
            .from("testSignal")
            .window(sliding(5, TimeUnit.MINUTES, 4))
            .snappedTimeRange(System.currentTimeMillis(), -3600000)
            .build();
    validator.validate(statement, 0, null);
  }
}
