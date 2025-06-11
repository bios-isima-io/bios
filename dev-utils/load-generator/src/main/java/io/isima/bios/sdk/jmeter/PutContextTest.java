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
import java.util.ArrayList;
import java.util.List;

import io.isima.bios.load.utils.DbUtils;
import io.isima.bios.sdk.Session;
import io.isima.bios.sdk.errors.BiosClientError;
import io.isima.bios.sdk.exceptions.BiosClientException;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

public class PutContextTest extends AbstractJmeterTest {

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

  public SampleResult runTest(JavaSamplerContext ctx) {
    final SampleResult result = new SampleResult();
    final List<String> contextEntries = new ArrayList<String>();
    long startTime = System.currentTimeMillis();
    String context = config.getStream(ctx.getParameter("stream")).getName();
    try {
      contextEntries.add(data.generateEvent(ctx.getParameter("stream")));
      DbUtils.putContextEntries(session, context, contextEntries);
      final long endTime = System.currentTimeMillis();
      result.setLatency(endTime - startTime);
      result.setStampAndTime(endTime, (endTime - startTime));
      result.setResponseCode("200");
      result.setSuccessful(true);
      result.setTimeStamp(System.currentTimeMillis());
      result.setSampleLabel("Put Context |" + ctx.getParameter("stream"));
      result.setResponseMessage(ApplicationUtils.getComputerName() + "|" +parseDate(endTime) + "|Put context successful");
    } catch (BiosClientException e) {
      if (e.getCode().equals(BiosClientError.UNAUTHORIZED)) {
        try {
          DbUtils.putContextEntries(session, context, contextEntries);
          final long endTime = System.currentTimeMillis();
          result.setLatency(endTime - startTime);
          result.setStampAndTime(endTime, (endTime - startTime));
          result.setResponseCode("200");
          result.setSuccessful(true);
          result.setResponseMessage(ApplicationUtils.getComputerName() + "|" +parseDate(endTime) + "|Put context successful");
          result.setSampleLabel("Put Context |" + ctx.getParameter("stream"));
        } catch (BiosClientException e1) {
          handleError(result, startTime, e1);
        }
      } else {
        handleError(result, startTime, e);
      }
    } catch (Exception e) {
      handleError(result, startTime, e);
    }
    loadLog.publishToBios(result);
    return result;
  }

}
