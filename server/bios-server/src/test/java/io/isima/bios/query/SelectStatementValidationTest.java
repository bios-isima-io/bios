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
import static io.isima.bios.models.isql.Metric.last;
import static io.isima.bios.models.isql.Metric.max;
import static io.isima.bios.models.isql.Metric.min;
import static io.isima.bios.models.isql.Metric.sum;
import static io.isima.bios.models.isql.OrderBy.by;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.codec.proto.isql.ProtoBuilderProvider;
import io.isima.bios.exceptions.validator.ConstraintViolationValidatorException;
import io.isima.bios.exceptions.validator.InvalidValueValidatorException;
import io.isima.bios.models.isql.ISqlStatement;
import io.isima.bios.models.isql.QueryValidator;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.utils.TfosObjectMapperProvider;
import org.hamcrest.junit.ExpectedException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

public class SelectStatementValidationTest {

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
            + "  ]"
            + "}";
    signal = mapper.readValue(src.replace("'", "\""), StreamConfig.class);
    validator = new TfosQueryValidator(signal, null);
  }

  @Test
  public void simplest() throws Exception {
    final var statement =
        ISqlStatement.select()
            .from("testSignal")
            .timeRange(System.currentTimeMillis(), -3600000)
            .build();
    validator.validate(statement, 0, null);
  }

  @Test
  public void selectAttributes() throws Exception {
    final var statement =
        ISqlStatement.select("county", "value")
            .from("testSignal")
            .timeRange(System.currentTimeMillis(), -3600000)
            .build();
    validator.validate(statement, 0, null);
  }

  @Test
  public void selectAndGroup() throws Exception {
    final var statement =
        ISqlStatement.select(count(), "county")
            .from("testSignal")
            .groupBy("county")
            .timeRange(System.currentTimeMillis(), -3600000)
            .build();
    validator.validate(statement, 0, null);
  }

  @Test
  public void selectNonExistingAttributes() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);

    final var statement =
        ISqlStatement.select("region")
            .from("testSignal")
            .timeRange(System.currentTimeMillis(), -3600000)
            .build();
    validator.validate(statement, 0, null);
  }

  @Test
  public void groupAndCount() throws Exception {
    final var statement =
        ISqlStatement.select(count())
            .from("testSignal")
            .groupBy("county")
            .timeRange(System.currentTimeMillis(), -3600000)
            .build();
    validator.validate(statement, 0, null);
  }

  @Test
  public void groupAndAggregates() throws Exception {
    final var statement =
        ISqlStatement.select(sum("value"), min("latitude"), max("longitude"), last("value"))
            .from("testSignal")
            .groupBy("county")
            .timeRange(System.currentTimeMillis(), -3600000)
            .build();
    validator.validate(statement, 0, null);
  }

  @Test
  public void aggregatesWithoutGroupSpec() throws Exception {
    final var statement =
        ISqlStatement.select(
                count(), min("value"), sum("longitude"), max("latitude"), last("county"))
            .from("testSignal")
            .timeRange(System.currentTimeMillis(), -3600000)
            .build();
    validator.validate(statement, 0, null);
  }

  @Test
  public void selectDuplicateAttributes() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);

    final var statement =
        ISqlStatement.select("country", sum("value"), "county")
            .from("testSignal")
            .timeRange(System.currentTimeMillis(), -3600000)
            .build();
    validator.validate(statement, 0, null);
  }

  @Test
  public void selectDuplicateAggregates() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);

    final var statement =
        ISqlStatement.select(sum("value"), "county", sum("value"))
            .from("testSignal")
            .timeRange(System.currentTimeMillis(), -3600000)
            .build();
    validator.validate(statement, 0, null);
  }

  @Test
  public void selectOutputNameConflict() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);

    final var statement =
        ISqlStatement.select(sum("value").as("abc"), "county", min("value").as("abc"))
            .from("testSignal")
            .timeRange(System.currentTimeMillis(), -3600000)
            .build();
    validator.validate(statement, 0, null);
  }

  @Test
  public void groupAndAggregateConflict() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);

    final var statement =
        ISqlStatement.select(sum("value").as("abc"))
            .from("testSignal")
            .groupBy("value")
            .timeRange(System.currentTimeMillis(), -3600000)
            .build();
    validator.validate(statement, 0, null);
  }

  @Test
  public void selectOutputNameConflict2() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);

    final var statement =
        ISqlStatement.select("county", sum("value").as("county"))
            .from("testSignal")
            .timeRange(System.currentTimeMillis(), -3600000)
            .build();
    validator.validate(statement, 0, null);
  }

  @Test
  public void duplicateGroups() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);

    final var statement =
        ISqlStatement.select(count())
            .from("testSignal")
            .groupBy("county", "city", "county")
            .timeRange(System.currentTimeMillis(), -3600000)
            .build();
    validator.validate(statement, 0, null);
  }

  @Test
  public void trySummingString() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    final var statement =
        ISqlStatement.select(sum("city"))
            .from("testSignal")
            .timeRange(System.currentTimeMillis(), -3600000)
            .build();
    validator.validate(statement, 0, null);
  }

  @Test
  public void minMaxLastString() throws Exception {
    final var statement =
        ISqlStatement.select(min("city"), max("city"), last("city"))
            .from("testSignal")
            .timeRange(System.currentTimeMillis(), -3600000)
            .build();
    validator.validate(statement, 0, null);
  }

  @Test
  public void trySummingBoolean() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    final var statement =
        ISqlStatement.select(sum("activated"))
            .from("testSignal")
            .timeRange(System.currentTimeMillis(), -3600000)
            .build();
    validator.validate(statement, 0, null);
  }

  @Test
  public void minMaxLastBoolean() throws Exception {
    final var statement =
        ISqlStatement.select(min("activated"), max("activated"), last("activated"))
            .from("testSignal")
            .timeRange(System.currentTimeMillis(), -3600000)
            .build();
    validator.validate(statement, 0, null);
  }

  @Test
  public void trySummingBlob() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    final var statement =
        ISqlStatement.select(sum("signature"))
            .from("testSignal")
            .timeRange(System.currentTimeMillis(), -3600000)
            .build();
    validator.validate(statement, 0, null);
  }

  @Test
  public void minMaxLastBlob() throws Exception {
    final var statement =
        ISqlStatement.select(min("signature"), max("signature"), last("signature"))
            .from("testSignal")
            .timeRange(System.currentTimeMillis(), -3600000)
            .build();
    validator.validate(statement, 0, null);
  }

  @Test
  public void trySummingEnum() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    final var statement =
        ISqlStatement.select(sum("status"))
            .from("testSignal")
            .timeRange(System.currentTimeMillis(), -3600000)
            .build();
    validator.validate(statement, 0, null);
  }

  @Test
  public void minMaxLastEnum() throws Exception {
    final var statement =
        ISqlStatement.select(min("status"), max("status"), last("status"))
            .from("testSignal")
            .timeRange(System.currentTimeMillis(), -3600000)
            .build();
    validator.validate(statement, 0, null);
  }

  @Test
  public void groupByNonExistingAttribute() throws Exception {
    thrown.expect(InvalidValueValidatorException.class);
    final var statement =
        ISqlStatement.select(sum("value"))
            .from("testSignal")
            .groupBy("region")
            .timeRange(System.currentTimeMillis(), -3600000)
            .build();
    validator.validate(statement, 0, null);
  }

  @Test
  public void sumOfNonExistingAttribute() throws Exception {
    thrown.expect(InvalidValueValidatorException.class);
    final var statement =
        ISqlStatement.select(sum("nosuchvalue"))
            .from("testSignal")
            .groupBy("country")
            .timeRange(System.currentTimeMillis(), -3600000)
            .build();
    validator.validate(statement, 0, null);
  }

  @Test
  public void minOfNonExistingAttribute() throws Exception {
    thrown.expect(InvalidValueValidatorException.class);
    final var statement =
        ISqlStatement.select(min("nosuchvalue"))
            .from("testSignal")
            .groupBy("country")
            .timeRange(System.currentTimeMillis(), -3600000)
            .build();
    validator.validate(statement, 0, null);
  }

  @Test
  public void maxOfNonExistingAttribute() throws Exception {
    thrown.expect(InvalidValueValidatorException.class);
    final var statement =
        ISqlStatement.select(max("nosuchvalue"))
            .from("testSignal")
            .groupBy("country")
            .timeRange(System.currentTimeMillis(), -3600000)
            .build();
    validator.validate(statement, 0, null);
  }

  @Test
  public void lastOfNonExistingAttribute() throws Exception {
    thrown.expect(InvalidValueValidatorException.class);
    final var statement =
        ISqlStatement.select(last("nosuchvalue"))
            .from("testSignal")
            .groupBy("country")
            .timeRange(System.currentTimeMillis(), -3600000)
            .build();
    validator.validate(statement, 0, null);
  }

  @Test
  public void groupAndAggregatesOrderByDimension() throws Exception {
    final var statement =
        ISqlStatement.select("county", min("value"))
            .from("testSignal")
            .groupBy("county")
            .orderBy(by("county").desc())
            .timeRange(System.currentTimeMillis(), -3600000)
            .build();
    validator.validate(statement, 0, null);
  }

  @Test
  public void groupAndAggregatesOrderByMeasurement() throws Exception {
    final var statement =
        ISqlStatement.select(min("value"))
            .from("testSignal")
            .groupBy("county")
            .orderBy(by("min(value)").desc())
            .timeRange(System.currentTimeMillis(), -3600000)
            .build();
    validator.validate(statement, 0, null);
  }

  @Test
  public void groupAndAggregatesOrderByAlias() throws Exception {
    final var statement =
        ISqlStatement.select(sum("value").as("total"))
            .from("testSignal")
            .groupBy("county")
            .orderBy(by("county").desc())
            .timeRange(System.currentTimeMillis(), -3600000)
            .build();
    validator.validate(statement, 0, null);
  }
}
