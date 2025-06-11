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
package io.isima.bios.sdk.jmeter;

import io.isima.bios.data.service.Config;
import io.isima.bios.data.service.DataStore;
import io.isima.bios.data.service.LoadClient;
import io.isima.bios.data.service.LoadLog;
import io.isima.bios.load.utils.ApplicationUtils;
import io.isima.bios.load.utils.DbUtils;
import io.isima.bios.models.Record;
import io.isima.bios.models.isql.ISqlResponse;
import io.isima.bios.sdk.Session;
import io.isima.bios.sdk.errors.BiosClientError;
import io.isima.bios.sdk.exceptions.BiosClientException;
import java.util.List;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

public class ExtractTest extends AbstractJmeterTest {

  private static LoadClient loadClient = null;
  private static Config config = null;
  private static DataStore data = null;
  private static LoadLog loadLog = null;
  private static Session session = null;

  @Override
  public void setupTest(JavaSamplerContext ctx) {
    super.setupTest(ctx);
  }

  public static void init(Config conf, LoadClient load, DataStore dataStore) {
    if (null == loadClient) {
      loadClient = load;
    }
    if (null == config) {
      config = conf;
    }
    if (null == data) {
      data = dataStore;
    }
    if (null != ApplicationUtils.getLogMetricsStreamName()) {
      loadLog =
          new LoadLog(loadClient.getUserSession());
    }
    session = loadClient.getUserSession();
  }

  @Override
  public SampleResult runTest(JavaSamplerContext ctx) {
    final SampleResult result = new SampleResult();
    long startTime = System.currentTimeMillis();
    String signal = config.getStream(ctx.getParameter("stream")).getName();
    long start = ApplicationUtils.getExtractStartTimeDelta();
    long delta = ApplicationUtils.getExtractInterval();
    try {
      ISqlResponse response = DbUtils.select(session, signal, start, delta);
      List<Record> events = response.getDataWindows().get(0).getRecords();
      final long endTime = System.currentTimeMillis();
      result.setLatency(endTime - startTime);
      result.setStampAndTime(endTime, (endTime - startTime));
      result.setResponseCode("200");
      result.setResponseMessage(ApplicationUtils.getComputerName() + "|" + parseDate(endTime)
          + "|Fetched " + events.size() + " events");
      result.setSuccessful(true);
      result.setTimeStamp(System.currentTimeMillis());
      result.setSampleLabel("Extract |" + ctx.getParameter("stream"));
    } catch (BiosClientException e) {
      // if client session has expired due to inactive session, do a login again
      if (e.getCode().equals(BiosClientError.UNAUTHORIZED)) {
        startTime = System.currentTimeMillis();
        try {
          ISqlResponse response = DbUtils.select(session, signal, start, delta);
          List<Record> events = response.getDataWindows().get(0).getRecords();
          final long endTime = System.currentTimeMillis();
          result.setLatency(endTime - startTime);
          result.setStampAndTime(endTime, (endTime - startTime));
          result.setResponseCode("200");
          result.setResponseMessage(ApplicationUtils.getComputerName() + "|" + parseDate(endTime)
              + "|Fetched " + events.size() + " events");
          result.setSampleLabel("Extract |" + ctx.getParameter("stream"));
        } catch (BiosClientException e1) {
          handleError(result, startTime, e1);
        }
        result.setSuccessful(true);
      } else {
        handleError(result, startTime, e);
      }
    } catch (Exception e) {
      handleError(result, startTime, e);
    }
    loadLog.publishToBios(result);
    return result;
  }

  @Override
  public void teardownTest(JavaSamplerContext ctx) {}

}
