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

import io.isima.bios.load.utils.ApplicationUtils;
import java.text.SimpleDateFormat;
import java.util.Date;

import io.isima.bios.sdk.errors.BiosClientError;
import io.isima.bios.sdk.exceptions.BiosClientException;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.samplers.SampleResult;

public abstract class AbstractJmeterTest extends AbstractJavaSamplerClient {
  private static final String DEFAULT_HOST = "localhost";
  private static final String DEFAULT_PORT = "443";
  private static final String DEFAULT_SSL_FLAG = "true";
  private static final String DEFAULT_CERT_FILE = "$SSL_CERT_FILE";

//  @Override
  public Arguments getDefaultParameters() {
    Arguments defaultParameters = new Arguments();
    defaultParameters.addArgument("streams_json", "$stream.json");
    defaultParameters.addArgument("host", DEFAULT_HOST);
    defaultParameters.addArgument("port", DEFAULT_PORT);
    defaultParameters.addArgument("ssl", DEFAULT_SSL_FLAG);
    defaultParameters.addArgument("cacert", DEFAULT_CERT_FILE);
    defaultParameters.addArgument("tenant", "$tenant");
    defaultParameters.addArgument("stream", "$stream");
    return defaultParameters;
  }

  public void handleError(SampleResult result, final long startTime, Exception ex) {
    result.setTimeStamp(System.currentTimeMillis());
    if (ex instanceof BiosClientException) {
      BiosClientException e = (BiosClientException) ex;
      result.setResponseMessage(
          ApplicationUtils.getComputerName() + "|" + parseDate(result.getTimeStamp())
              + "|" + e.getMessage());
      result.setResponseCode(e.getCode().name());
      if (e.getCode().equals(BiosClientError.SERVER_CHANNEL_ERROR)
          || e.getCode().equals(BiosClientError.NO_SUCH_TENANT)) {
        result.setStopThread(true);
      }
    } else {
      result.setResponseMessage(
          ApplicationUtils.getComputerName() + "|" + parseDate(result.getTimeStamp())
              + "|" + ex.getMessage());
      result.setResponseCode("500");
    }
    result.setLatency(System.currentTimeMillis() - startTime);
    result.setStampAndTime(System.currentTimeMillis(), (System.currentTimeMillis() - startTime));

    result.setSuccessful(false);
    String message = ex.getMessage();
    boolean supressLog = false;
    for (var logMsg : ApplicationUtils.getLogSkipMessages()) {
      if (message.contains(logMsg.strip())) {
        supressLog = true;
        break;
      }
    }
    if (!supressLog) {
      getNewLogger().error(message);
    }
  }

  public final String parseDate(final long date) {
    SimpleDateFormat df = new SimpleDateFormat("dd-MM-yyyy HH:mm");
    return df.format(new Date(date)).toString();
  }

}
