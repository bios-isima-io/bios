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

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.admin.v1.impl.AdminImpl;
import io.isima.bios.data.DataEngine;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.AdminTestUtils;
import io.isima.bios.it.tools.TestUtils;
import io.isima.bios.models.Count;
import io.isima.bios.models.Event;
import io.isima.bios.models.Max;
import io.isima.bios.models.Min;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.Sum;
import io.isima.bios.models.SummarizeRequest;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.server.handlers.DataServiceHandler;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

/** Helper class to setup a summarize test. */
public class SummarizeTestSetup {

  /**
   * Class to carry parameters generated during the test setup.
   *
   * <p>The parameters will be used by test cases.
   */
  public class Params {
    /** Fundamental summarization interval. */
    long basicInterval;

    /** Largest summarization interval. */
    long largestInterval;

    /** First ingest timestamp. */
    long firstIngest;

    /** StreamDesc of the generated stream for the test. */
    StreamDesc streamDesc;

    // The setup method executes the Summarize API four times between the initial
    // and second ingestions. Following parameters keep records of the executions.
    SummarizeRequest request1st;
    Map<Long, List<Event>> out1st;

    SummarizeRequest request2nd;
    Map<Long, List<Event>> out2nd;

    SummarizeRequest request3rd;
    Map<Long, List<Event>> out3rd;

    SummarizeRequest request4th;
    Map<Long, List<Event>> out4th;
  }

  private final AdminImpl admin;
  private final DataServiceHandler dataServiceHandler;
  private final DataEngine dataEngine;

  private long timestamp;
  private final String testTenant;

  /** The constructor sets up necessary modules and initial timestamp. */
  public SummarizeTestSetup(String testTenant) {
    admin = (AdminImpl) BiosModules.getAdminInternal();
    dataServiceHandler = BiosModules.getDataServiceHandler();
    dataEngine = BiosModules.getDataEngine();

    timestamp = System.currentTimeMillis();
    this.testTenant = testTenant;
  }

  /**
   * The setup method creates the test stream and ingests two chunks of events temporaly separated
   * by basicInterval.
   *
   * @param signalConfigSrc Test stream source string
   * @param signalName Test signal name
   * @return Setup parameters
   * @throws Throwable for any exceptions
   */
  public Params setUp(String signalConfigSrc, String signalName) throws Throwable {
    final Params params = new Params();
    params.basicInterval = 60000;
    final long basicInterval = params.basicInterval;
    params.largestInterval = basicInterval * 2;
    final long largestInterval = params.largestInterval;
    long timestamp = System.currentTimeMillis() - largestInterval * 2;
    if (timestamp % basicInterval > basicInterval - 20000) { // avoid boundary
      timestamp += 25000;
    }
    if (timestamp % largestInterval > basicInterval) {
      timestamp += largestInterval - basicInterval;
    }

    TenantDesc temp = new TenantDesc(testTenant, timestamp, Boolean.FALSE);
    try {
      admin.removeTenant(temp.toTenantConfig(), RequestPhase.INITIAL, timestamp);
      admin.removeTenant(temp.toTenantConfig(), RequestPhase.FINAL, timestamp);
    } catch (TfosException e) {
      // ignore
    }
    admin.addTenant(temp.toTenantConfig(), RequestPhase.INITIAL, temp.getVersion());
    admin.addTenant(temp.toTenantConfig(), RequestPhase.FINAL, temp.getVersion());

    AdminTestUtils.populateStream(admin, testTenant, signalConfigSrc, ++timestamp);

    StreamDesc streamDesc = admin.getStream(testTenant, signalName);
    params.streamDesc = streamDesc;

    // ingest the initial chunk of events
    final String[] source = {
      "Alameda,Fremont,DEER,5,15",
      "Marin,Inverness,ELK,9,20",
      "San Mateo,San Carlos,COYOTE,1,5",
      "Santa Clara,Alum Rock,DEER,4,20",
      "San Mateo,Belmont,DEER,5,90",
      "Alameda,Sunol,BEAR,2,60",
      "Marin,Inverness,ELK,12,40",
      "San Mateo,Belmont,DEER,7,70",
    };
    final var resp =
        TestUtils.insert(dataServiceHandler, testTenant, streamDesc, ++timestamp, source[0]);
    for (int i = 1; i < source.length; ++i) {
      TestUtils.insert(dataServiceHandler, testTenant, streamDesc, ++timestamp, source[i]);
    }

    params.firstIngest = resp.getTimeStamp() / basicInterval * basicInterval;
    System.out.println(
        "\n### start: " + new Date(params.firstIngest) + " (" + params.firstIngest + ")");

    // 1st step: summarize the first chunk immediately
    {
      System.out.println("1st step");
      final SummarizeRequest request = new SummarizeRequest();
      request.setStartTime(params.firstIngest);
      request.setEndTime(params.firstIngest + basicInterval);
      request.setInterval(basicInterval);
      request.setGroup(Arrays.asList("county"));
      request.setAggregates(
          Arrays.asList(new Count(), new Sum("number"), new Min("distance"), new Max("distance")));

      params.request1st = request;
      params.out1st = TestUtils.executeSummarize(dataEngine, streamDesc, request);
    }

    // sleep, then rollup would be done if any
    timestamp += basicInterval;
    ((DataEngineImpl) BiosModules.getDataEngine())
        .getPostProcessScheduler()
        .setRollupStopTime(timestamp);
    Thread.sleep(12000);

    // 2nd step: summarize the first chunk again after sleeping for a while
    {
      System.out.println("2nd step");
      final SummarizeRequest request = new SummarizeRequest();
      request.setStartTime(params.firstIngest);
      request.setEndTime(params.firstIngest + basicInterval);
      request.setInterval(basicInterval);
      request.setGroup(Arrays.asList("county"));
      request.setAggregates(
          Arrays.asList(new Count(), new Sum("number"), new Min("distance"), new Max("distance")));

      params.request2nd = request;
      params.out2nd = TestUtils.executeSummarize(dataEngine, streamDesc, request);
    }

    // 3rd step: specify the output names and retry
    {
      System.out.println("3rd step");
      final SummarizeRequest request = new SummarizeRequest();
      request.setStartTime(params.firstIngest);
      request.setEndTime(params.firstIngest + basicInterval);
      request.setInterval(basicInterval);
      request.setGroup(Arrays.asList("county"));
      request.setAggregates(
          Arrays.asList(
              new Count().as("sightingsCount"),
              new Sum("number").as("animals_count"),
              new Min("distance").as("minimumDistance"),
              new Max("distance").as("maximumDistance")));

      params.request3rd = request;
      params.out3rd = TestUtils.executeSummarize(dataEngine, streamDesc, request);
    }

    // ingest the second chunk of events
    final String[] source2 = {
      "Alameda,Livermore,MOUNTAIN_LION,1,80",
      "Marin,Muir Beach,DEER,5,10",
      "San Mateo,Redwood City,COYOTE,1,5",
      "Alameda,Livermore,BEAR,1,50",
      "Santa Clara,Monte Sereno,DEER,3,90",
      "San Mateo,Belmont,DEER,5,90",
      "Alameda,Sunol,BEAR,2,60",
      "Marin,Muir Beach,COYOTE,1,40",
      "San Mateo,Redwood City,DEER,4,20",
    };
    for (String eventText : source2) {
      TestUtils.insert(dataServiceHandler, testTenant, streamDesc, ++timestamp, eventText);
    }

    // 4th step: summarize two chunks of events immediately after the second cluster of ingestions
    {
      System.out.println("4th step");
      final SummarizeRequest request = new SummarizeRequest();
      request.setStartTime(params.firstIngest);
      request.setEndTime(params.firstIngest + basicInterval * 2);
      request.setInterval(basicInterval);
      request.setGroup(Arrays.asList("county"));
      request.setAggregates(
          Arrays.asList(new Count(), new Sum("number"), new Min("distance"), new Max("distance")));

      params.request4th = request;
      params.out4th = TestUtils.executeSummarize(dataEngine, streamDesc, request);
    }

    // sleep, then rollup is done
    timestamp += basicInterval;
    ((DataEngineImpl) BiosModules.getDataEngine())
        .getPostProcessScheduler()
        .setRollupStopTime(timestamp);
    Thread.sleep(12000);

    return params;
  }

  /** Method to tead down the test environment. */
  public void tearDown() throws Exception {
    admin.removeTenant(
        new TenantConfig(testTenant).setVersion(++timestamp), RequestPhase.INITIAL, timestamp);
    admin.removeTenant(
        new TenantConfig(testTenant).setVersion(timestamp), RequestPhase.FINAL, timestamp);
  }
}
