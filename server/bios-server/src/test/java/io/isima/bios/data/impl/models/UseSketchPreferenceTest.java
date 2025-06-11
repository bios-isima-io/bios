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
package io.isima.bios.data.impl.models;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.ContextQueryState;
import io.isima.bios.dto.SelectContextRequest;
import io.isima.bios.dto.SelectOrder;
import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.models.Avg;
import io.isima.bios.models.Count;
import io.isima.bios.models.DistinctCount;
import io.isima.bios.models.GenericMetric;
import io.isima.bios.models.Max;
import io.isima.bios.models.MetricFunction;
import io.isima.bios.models.Min;
import io.isima.bios.models.Sort;
import io.isima.bios.models.Sum;
import io.isima.bios.models.SummarizeRequest;
import io.isima.bios.models.v1.Aggregate;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.StreamConfig;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Test;

public class UseSketchPreferenceTest {
  private static StreamDesc streamDesc;

  @BeforeClass
  public static void setUpClass() {
    final StreamConfig streamConfig = new StreamConfig("testSignal");
    streamConfig.addAttribute(new AttributeDesc("stringAttr", InternalAttributeType.STRING));
    streamConfig.addAttribute(new AttributeDesc("integerAttr", InternalAttributeType.LONG));
    streamConfig.addAttribute(new AttributeDesc("decimalAttr", InternalAttributeType.DOUBLE));
    streamConfig.addAttribute(new AttributeDesc("booleanAttr", InternalAttributeType.BOOLEAN));
    streamConfig.addAttribute(new AttributeDesc("blobAttr", InternalAttributeType.BLOB));
    streamDesc = new StreamDesc(streamConfig);
  }

  @Test
  public void simpleFeatures() throws InvalidRequestException {
    var request = new SummarizeRequest();
    request.setAggregates(
        List.of(
            new Count(),
            new Sum("integerAttr"),
            new Min("decimalAttr"),
            new Max("integerAttr"),
            new Avg("decimalAttr"),
            new DistinctCount("stringAttr")));
    request.setGroup(List.of());

    var preference = UseSketchPreference.decide(request, streamDesc);
    assertThat(preference, is(new UseSketchPreference(true, false, true, "", "")));
  }

  private GenericMetric count() {
    return new GenericMetric(MetricFunction.COUNT, null, null);
  }

  private GenericMetric sum(String of) {
    return new GenericMetric(MetricFunction.SUM, of, null);
  }

  private GenericMetric min(String of) {
    return new GenericMetric(MetricFunction.MIN, of, null);
  }

  private GenericMetric max(String of) {
    return new GenericMetric(MetricFunction.MAX, of, null);
  }

  private GenericMetric avg(String of) {
    return new GenericMetric(MetricFunction.AVG, of, null);
  }

  private GenericMetric distinctcount(String of) {
    return new GenericMetric(MetricFunction.DISTINCTCOUNT, of, null);
  }

  private ContextQueryState makeState(SelectContextRequest request) {
    final var queryState = new ContextQueryState();
    queryState.setAggregates(
        request.getMetrics().stream()
            .map((metric) -> new Aggregate(metric.getFunction(), metric.getOf(), metric.getAs()))
            .collect(Collectors.toList()));
    return queryState;
  }

  @Test
  public void simpleContextFeatures() throws InvalidRequestException {
    var request = new SelectContextRequest();
    request.setMetrics(
        List.of(
            count(),
            sum("integerAttr"),
            min("decimalAttr"),
            max("integerAttr"),
            avg("decimalAttr"),
            distinctcount("stringAttr")));

    var preference = UseSketchPreference.decide(request, makeState(request), streamDesc);
    assertThat(preference, is(new UseSketchPreference(true, false, true, "", "")));
  }

  @Test
  public void featuresWithDimensions() throws InvalidRequestException {
    var request = new SummarizeRequest();
    request.setAggregates(
        List.of(
            new Count(),
            new Sum("integerAttr"),
            new Min("decimalAttr"),
            new Max("integerAttr"),
            new Avg("decimalAttr"),
            new DistinctCount("stringAttr")));
    request.setGroup(List.of("stringAttribute"));

    var preference = UseSketchPreference.decide(request, streamDesc);
    assertThat(
        preference,
        is(new UseSketchPreference(false, false, false, "", "Group by: [stringAttribute]")));
  }

  @Test
  public void contextFeaturesWithDimensions() throws InvalidRequestException {
    var request = new SelectContextRequest();
    request.setMetrics(
        List.of(
            count(),
            sum("integerAttr"),
            min("decimalAttr"),
            max("integerAttr"),
            avg("decimalAttr"),
            distinctcount("stringAttr")));
    request.setGroupBy(List.of("stringAttribute"));

    var preference = UseSketchPreference.decide(request, makeState(request), streamDesc);
    assertThat(
        preference,
        is(new UseSketchPreference(false, false, false, "", "Group by: [stringAttribute]")));
  }

  @Test
  public void featuresWithSorting() throws InvalidRequestException {
    var request = new SummarizeRequest();
    request.setAggregates(
        List.of(
            new Count(),
            new Sum("integerAttr"),
            new Min("decimalAttr"),
            new Max("integerAttr"),
            new Avg("decimalAttr"),
            new DistinctCount("stringAttr")));
    request.setGroup(List.of());
    request.setSort(new Sort("stringAttr"));

    var preference = UseSketchPreference.decide(request, streamDesc);
    assertThat(
        preference, is(new UseSketchPreference(false, false, false, "", "Sort by stringAttr")));
  }

  @Test
  public void contextFeaturesWithSorting() throws InvalidRequestException {
    var request = new SelectContextRequest();
    request.setMetrics(
        List.of(
            count(),
            sum("integerAttr"),
            min("decimalAttr"),
            max("integerAttr"),
            avg("decimalAttr"),
            distinctcount("stringAttr")));
    request.setGroupBy(List.of());
    request.setOrderBy(new SelectOrder("stringAttr", false, false));

    var preference = UseSketchPreference.decide(request, makeState(request), streamDesc);
    assertThat(
        preference, is(new UseSketchPreference(false, false, false, "", "Sort by stringAttr")));
  }

  @Test
  public void featuresWithLimit() throws InvalidRequestException {
    var request = new SummarizeRequest();
    request.setAggregates(
        List.of(
            new Count(),
            new Sum("integerAttr"),
            new Min("decimalAttr"),
            new Max("integerAttr"),
            new Avg("decimalAttr"),
            new DistinctCount("stringAttr")));
    request.setGroup(List.of());
    request.setLimit(10);

    var preference = UseSketchPreference.decide(request, streamDesc);
    assertThat(preference, is(new UseSketchPreference(false, false, false, "", "Limit: 10")));
  }

  @Test
  public void contextFeaturesWithLimit() throws InvalidRequestException {
    var request = new SelectContextRequest();
    request.setMetrics(
        List.of(
            count(),
            sum("integerAttr"),
            min("decimalAttr"),
            max("integerAttr"),
            avg("decimalAttr"),
            distinctcount("stringAttr")));
    request.setGroupBy(List.of());
    request.setLimit(10);

    var preference = UseSketchPreference.decide(request, makeState(request), streamDesc);
    assertThat(preference, is(new UseSketchPreference(false, false, false, "", "Limit: 10")));
  }

  @Test
  public void featuresWithFilter() throws InvalidRequestException {
    var request = new SummarizeRequest();
    request.setAggregates(
        List.of(
            new Count(),
            new Sum("integerAttr"),
            new Min("decimalAttr"),
            new Max("integerAttr"),
            new Avg("decimalAttr"),
            new DistinctCount("stringAttr")));
    request.setGroup(List.of());
    request.setFilter("booleanAttr = false");

    var preference = UseSketchPreference.decide(request, streamDesc);
    assertThat(
        preference,
        is(new UseSketchPreference(false, false, false, "", "Filter: booleanAttr = false")));
  }

  @Test
  public void contextFeaturesWithFilter() throws InvalidRequestException {
    var request = new SelectContextRequest();
    request.setMetrics(
        List.of(
            count(),
            sum("integerAttr"),
            min("decimalAttr"),
            max("integerAttr"),
            avg("decimalAttr"),
            distinctcount("stringAttr")));
    request.setGroupBy(List.of());
    request.setWhere("booleanAttr = false");

    var preference = UseSketchPreference.decide(request, makeState(request), streamDesc);
    assertThat(
        preference,
        is(new UseSketchPreference(false, false, false, "", "Filter: booleanAttr = false")));
  }

  @Test
  public void featuresWithSlidingWindow() throws InvalidRequestException {
    var request = new SummarizeRequest();
    request.setAggregates(
        List.of(
            new Count(),
            new Sum("integerAttr"),
            new Min("decimalAttr"),
            new Max("integerAttr"),
            new Avg("decimalAttr"),
            new DistinctCount("stringAttr")));
    request.setGroup(List.of());
    request.setInterval(300000L);
    request.setHorizon(600000L);

    var preference = UseSketchPreference.decide(request, streamDesc);
    assertThat(
        preference,
        is(
            new UseSketchPreference(
                false, false, false, "", "Sliding window: horizon 600000 != interval 300000")));
  }

  @Test
  public void complexMoments() throws InvalidRequestException {
    var request = new SummarizeRequest();
    request.setAggregates(
        List.of(new Count(), new Aggregate(MetricFunction.VARIANCE, "decimalAttr")));
    request.setGroup(List.of());

    var preference = UseSketchPreference.decide(request, streamDesc);
    assertThat(preference, is(new UseSketchPreference(true, true, true, "Aggregate VARIANCE", "")));
  }

  @Test
  public void complexContextMoments() throws InvalidRequestException {
    var request = new SelectContextRequest();
    final var variance = new GenericMetric(MetricFunction.VARIANCE, "decimalAttr", null);
    request.setMetrics(List.of(count(), variance));
    request.setGroupBy(List.of());

    var preference = UseSketchPreference.decide(request, makeState(request), streamDesc);
    assertThat(preference, is(new UseSketchPreference(true, true, true, "Aggregate VARIANCE", "")));
  }

  @Test
  public void invalidMoments() {
    var request = new SummarizeRequest();
    request.setAggregates(
        List.of(new Count(), new Aggregate(MetricFunction.VARIANCE, "stringAttr")));
    request.setGroup(List.of());

    assertThrows(
        InvalidRequestException.class, () -> UseSketchPreference.decide(request, streamDesc));
  }

  @Test
  public void momentsWithDimensions() {
    var request = new SummarizeRequest();
    request.setAggregates(
        List.of(new Count(), new Aggregate(MetricFunction.VARIANCE, "integerAttr")));
    request.setGroup(List.of("stringAttr"));

    assertThrows(
        InvalidRequestException.class, () -> UseSketchPreference.decide(request, streamDesc));
  }
}
