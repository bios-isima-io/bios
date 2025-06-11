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
package io.isima.bios.data.service;

import io.isima.bios.load.utils.ApplicationUtils;

import java.util.concurrent.CompletableFuture;

import io.isima.bios.load.utils.DbUtils;
import io.isima.bios.sdk.Session;
import io.isima.bios.sdk.exceptions.BiosClientException;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.log4j.Logger;

/**
 * @author sourav
 * <p>
 * This class is responsible to parse jmeter log and publish it to tfos against log stream
 */
public class LoadLog {
  private final static Logger logger = Logger.getLogger(LoadLog.class);
  private final static String LOG_METRICS_SIGNAL_NAME = ApplicationUtils.getLogMetricsStreamName();
  private Session session;

  public LoadLog(Session session) {
    this.session = session;
  }

  public void publishToBios(SampleResult result) {
    if (!ApplicationUtils.getPublishLog()) {
      return;
    }
    CompletableFuture.runAsync(new Runnable() {
      @Override
      public void run() {
        try {
          String event = result.getTimeStamp() + "," + result.getLatency() + ","
                  + result.getSampleLabel().replaceAll("\\|", ",")
                  + "," + result.getResponseCode()
                  + "," + result.getResponseMessage().replaceAll("\\|", ",")
                  + "," + result.getThreadName() + ",," + result.isSuccessful() + ",,,,"
                  + result.getGroupThreads() + "," + result.getAllThreads() + ",,"
                  + result.getLatency() + "," + result.getIdleTime()
                  + "," + result.getConnectTime();
          DbUtils.insert(session, LOG_METRICS_SIGNAL_NAME, event);
        } catch (BiosClientException e) {
          logger.error(e.getMessage(), e);
        } catch (Throwable th) {
          logger.error(th.getMessage(), th);
        }
      }
    });
  }
};
