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
package io.isima.bios.models;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.models.proto.DataProto;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

/** UT case for the class {@link SelectRequest}. */
public class SelectRequestTest {

  /** Tests the simple extraction request builder method. */
  @Test
  public void testSimpleExtractionRequestBuilder() {
    final String signalName = "testSignal";
    final long end = System.currentTimeMillis();
    final long start = end - 60000;
    final SelectRequest request = SelectRequest.simpleExtraction(signalName, new Range(start, end));
    assertNotNull(request);
    assertThat(request.getFrom(), is(signalName));
    assertThat(request.getTimeRange(), is(new Range(start, end)));
    assertNotNull(request.getWindow());
    assertThat(request.getWindow().getType(), is(WindowType.GLOBAL));
    assertTrue(request.getAttributes().isEmpty());
    assertTrue(request.getMetrics().isEmpty());
    assertTrue(request.getDimensions().isEmpty());
    assertTrue(request.getFilter().isEmpty());
    assertTrue(request.getOrder().isEmpty());
    assertThat(request.getLimit(), is(0L));
  }

  @Test
  public void testSimpleExtractionFromProto() throws Exception {
    final String signalName = "testSignalFromProto";
    final long end = System.currentTimeMillis();
    final long start = end - 60000;
    final DataProto.SelectQuery query =
        DataProto.SelectQuery.newBuilder()
            .setFrom(signalName)
            .setStartTime(start)
            .setEndTime(end)
            .build();
    final SelectRequest request = SelectRequest.fromProto(query);
    assertNotNull(request);
    assertThat(request.getFrom(), is(signalName));
    assertThat(request.getTimeRange(), is(new Range(start, end)));
    assertNotNull(request.getWindow());
    assertThat(request.getWindow().getType(), is(WindowType.GLOBAL));
    assertTrue(request.getAttributes().isEmpty());
    assertTrue(request.getMetrics().isEmpty());
    assertTrue(request.getDimensions().isEmpty());
    assertTrue(request.getFilter().isEmpty());
    assertTrue(request.getOrder().isEmpty());
    assertThat(request.getLimit(), is(0L));
  }

  @Test
  public void testComplexGlobalSelectFromProto() throws Exception {
    final String signalName = "testSignalFromProto";
    final long end = System.currentTimeMillis();
    final long start = end - 60000;
    final DataProto.SelectQuery query =
        DataProto.SelectQuery.newBuilder()
            .setFrom(signalName)
            .setStartTime(start)
            .setEndTime(end)
            .setGroupBy(DataProto.Dimensions.newBuilder().addAllDimensions(List.of("1st", "2nd")))
            .setAttributes(
                DataProto.AttributeList.newBuilder()
                    .addAllAttributes(List.of("attrA", "attrB", "attrC")))
            .setWhere("abc = 123")
            .setOrderBy(DataProto.OrderBy.newBuilder().setBy("attrB").setCaseSensitive(true))
            .setLimit(10)
            .build();
    final SelectRequest request = SelectRequest.fromProto(query);
    assertNotNull(request);
    assertThat(request.getFrom(), is(signalName));
    assertThat(request.getTimeRange(), is(new Range(start, end)));
    assertNotNull(request.getWindow());
    assertThat(request.getWindow().getType(), is(WindowType.GLOBAL));
    assertThat(request.getDimensions().size(), is(2));
    assertThat(request.getDimensions().get(0), is("1st"));
    assertThat(request.getDimensions().get(1), is("2nd"));
    assertTrue(request.getAttributes().isPresent());
    assertThat(request.getAttributes().get().size(), is(3));
    assertThat(request.getAttributes().get().get(0), is("attrA"));
    assertThat(request.getAttributes().get().get(1), is("attrB"));
    assertThat(request.getAttributes().get().get(2), is("attrC"));
    assertTrue(request.getFilter().isPresent());
    assertThat(request.getFilter().get(), is("abc = 123"));
    assertTrue(request.getOrder().isPresent());
    assertThat(request.getOrder().get(), is(new SelectOrder("attrB", false, true)));
    assertThat(request.getLimit(), is(10L));
  }

  @Test
  public void testForProtoEmptyDimensions() throws Exception {
    final String signalName = "testSignalFromProto";
    final long end = System.currentTimeMillis();
    final long start = end - 60000;
    final DataProto.SelectQuery query =
        DataProto.SelectQuery.newBuilder()
            .setFrom(signalName)
            .setStartTime(start)
            .setEndTime(end)
            .setGroupBy(DataProto.Dimensions.newBuilder().addAllDimensions(List.of()))
            .build();
    final SelectRequest request = SelectRequest.fromProto(query);
    assertNotNull(request);
    assertThat(request.getFrom(), is(signalName));
    assertThat(request.getTimeRange(), is(new Range(start, end)));
    assertNotNull(request.getWindow());
    assertThat(request.getWindow().getType(), is(WindowType.GLOBAL));
    assertTrue(request.getAttributes().isEmpty());
    assertTrue(request.getDimensions().isEmpty());
    assertTrue(request.getFilter().isEmpty());
  }

  @Test
  public void testComplexTumblingWindowQueryFromProto() throws Exception {
    final String signalName = "testSignalFromProto";
    final long end = System.currentTimeMillis();
    final long start = end - 60000;
    final DataProto.SelectQuery query =
        DataProto.SelectQuery.newBuilder()
            .setFrom(signalName)
            .setStartTime(start)
            .setEndTime(end)
            .setGroupBy(DataProto.Dimensions.newBuilder().addAllDimensions(List.of("1st", "2nd")))
            .addMetrics(
                DataProto.Metric.newBuilder()
                    .setFunction(DataProto.MetricFunction.SUM)
                    .setOf("sales")
                    .setAs("SumOfSales"))
            .addMetrics(DataProto.Metric.newBuilder().setFunction(DataProto.MetricFunction.COUNT))
            .addMetrics(
                DataProto.Metric.newBuilder()
                    .setFunction(DataProto.MetricFunction.LAST)
                    .setOf("visitorId"))
            .setWhere("abc = 123")
            .addWindows(
                DataProto.Window.newBuilder()
                    .setWindowType(DataProto.WindowType.TUMBLING_WINDOW)
                    .setTumbling(DataProto.TumblingWindow.newBuilder().setWindowSizeMs(300000)))
            .setOrderBy(DataProto.OrderBy.newBuilder().setBy("count()").setReverse(true))
            .setLimit(10)
            .build();
    final SelectRequest request = SelectRequest.fromProto(query);
    assertNotNull(request);
    assertThat(request.getFrom(), is(signalName));
    assertThat(request.getTimeRange(), is(new Range(start, end)));
    assertNotNull(request.getWindow());
    assertThat(request.getWindow().getType(), is(WindowType.TUMBLING));
    assertThat(request.getWindow().getWindowSizeInMillis(), is(300000L));
    assertThat(request.getWindow().getStepSizeInMillis(), is(300000L));
    assertTrue(request.getAttributes().isEmpty());
    assertThat(request.getMetrics().size(), is(3));
    assertThat(
        request.getMetrics().get(0),
        is(
            new SelectMetric(
                MeasurementFunction.SUM, Optional.of("sales"), Optional.of("SumOfSales"))));
    assertThat(
        request.getMetrics().get(1),
        is(new SelectMetric(MeasurementFunction.COUNT, Optional.empty(), Optional.empty())));
    assertThat(
        request.getMetrics().get(2),
        is(new SelectMetric(MeasurementFunction.LAST, Optional.of("visitorId"), Optional.empty())));
    assertThat(request.getDimensions().size(), is(2));
    assertThat(request.getDimensions().get(0), is("1st"));
    assertThat(request.getDimensions().get(1), is("2nd"));
    assertTrue(request.getFilter().isPresent());
    assertThat(request.getFilter().get(), is("abc = 123"));
    assertThat(request.getOrder().get(), is(new SelectOrder("count()", true, false)));
    assertThat(request.getLimit(), is(10L));
  }

  @Test
  public void testInvalidProtoSignalUnspecified() throws Exception {
    final long end = System.currentTimeMillis();
    final long start = end - 1;
    final DataProto.SelectQuery query =
        DataProto.SelectQuery.newBuilder().setStartTime(start).setEndTime(end).build();
    assertThrows(InvalidRequestException.class, () -> SelectRequest.fromProto(query));
  }

  @Test
  public void testInvalidProtoTimeRangeReversed() throws Exception {
    final String signalName = "testSignalFromProto";
    final long start = System.currentTimeMillis();
    final long end = start - 1;
    final DataProto.SelectQuery query =
        DataProto.SelectQuery.newBuilder()
            .setFrom(signalName)
            .setStartTime(start)
            .setEndTime(end)
            .build();
    assertThrows(InvalidRequestException.class, () -> SelectRequest.fromProto(query));
  }
}
