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

import com.strobel.assembler.Collection;
import io.isima.bios.data.distribution.GaussianDistribution;
import io.isima.bios.exception.ContextKeyNotFoundException;
import io.isima.bios.load.model.AttributeDesc;
import io.isima.bios.load.model.StreamConfig;
import io.isima.bios.load.model.StreamType;
import io.isima.bios.load.utils.ApplicationUtils;
import io.isima.bios.load.utils.DbUtils;
import io.isima.bios.load.utils.LoadPattern;
import io.isima.bios.models.Record;
import io.isima.bios.models.isql.ISqlResponse;
import io.isima.bios.sdk.Session;
import io.isima.bios.sdk.errors.BiosClientError;
import io.isima.bios.sdk.exceptions.BiosClientException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

public class GaussianLoad {

  private static Logger logger = Logger.getLogger(GaussianLoad.class);
  private Config config;
  private DataStore data;
  private Session userSession;

  public GaussianLoad(final Session userSession,
      final Config config, final DataStore data) {
    this.config = config;
    this.data = data;
    this.userSession = userSession;
  }

  public ScheduledExecutorService initExecutors(int noThreads, long taskDuration) {
    return new ScheduledThreadPoolExecutor(noThreads);
  }

  public Collection<Runnable> getExecutorThreads() {
    Collection<Runnable> ingestTask = new Collection<>();
    Set<String> streams = config.getStreamNames();
    for (String stream : streams) {
      StreamConfig streamConfig = config.getStream(stream);
      Runnable runnableTask = () -> {
        long[] requestSize = new long[ApplicationUtils.getGaussianTimeWindow()];
        if (ApplicationUtils.getLoadPattern().equals(LoadPattern.EXPONENTAIL)) {
          requestSize =
              ApplicationUtils.getExponentialDistribution(ApplicationUtils.getGaussianTimeWindow(),
                  streamConfig.getPeakRequestSize());
        } else {
          requestSize =
              GaussianDistribution.getRequestArray(ApplicationUtils.getGaussianTimeWindow(),
                  streamConfig.getPeakRequestSize());
        }

        for (int i = 0; i < requestSize.length; i++) {
          long avgSleepDelay = (int) ((1000 * 60 * 5) / requestSize[i]);
          for (int requestCount = 0; requestCount < requestSize[i]; requestCount++) {
            try {
              if (streamConfig.getType().equals(StreamType.CONTEXT)) {
                final String event = data.generateEvent(stream);
                DbUtils.putContextEntries(userSession, streamConfig.getName(),
                    Arrays.asList(event));
              } else {
                try {
                  final ISqlResponse insert =
                      DbUtils.insert(userSession, streamConfig.getName(),
                          data.generateEvent(stream));
                } catch (Throwable t) {
                  logger.error("----------------------------------------------------");
                  logger.error(t.getMessage(), t);
                  logger.error("----------------------------------------------------");
                }
              }
              TimeUnit.MILLISECONDS.sleep(avgSleepDelay);

            } catch (BiosClientException e) {
              handleBiosClientError(e);
            } catch (Exception e) {
              logger.error(e.getMessage(), e);
            }
          }
          if (streamConfig.getType().equals(StreamType.CONTEXT)) {
            logger.info("PUBLISHED " + requestSize[i] + " PUT CONTEXT requests to " + stream);
          } else {
            logger.info("PUBLISHED " + requestSize[i] + " INSERT requests to " + stream);
          }
        }
      };
      if (streamConfig.getType().equals(StreamType.CONTEXT)) {
//        Runnable updateContextTask = getUpdateContextTask(streamConfig);
        Runnable getContextTask = getContextItem(streamConfig);
//        ingestTask.add(updateContextTask);
        ingestTask.add(getContextTask);
      }
      ingestTask.add(runnableTask);
      ingestTask.add(getExtractExecutorThreads(streamConfig));
    }
    return ingestTask;
  }

  private Runnable getContextItem(final StreamConfig streamConfig) {
    Runnable getContextTask = () -> {
      long[] requestSize =
          GaussianDistribution.getRequestArray(ApplicationUtils.getGaussianTimeWindow(),
              Math.floorDiv(streamConfig.getPeakRequestSize(), 10));

      for (long requests : requestSize) {
        long avgSleepDelay = (int) ((1000 * 60 * 5) / requests);
        for (int requestCount = 0; requestCount < requests; requestCount++) {
          try {
            String context = streamConfig.getName();
            final String key = data.getContextkey(streamConfig.getAttributes().get(0).getName(),
                streamConfig.getName());
            DbUtils.getContextEntries(userSession, context, key);
            TimeUnit.MILLISECONDS.sleep(avgSleepDelay);
          } catch (BiosClientException e) {
            handleBiosClientError(e);
          } catch (ContextKeyNotFoundException cne) {
            logger.error(cne.getMessage());
          } catch (Exception e) {
            logger.error(e.getMessage(), e);
          }
        }
        logger.info("PUBLISHED " + requests + " GET CONTEXT requests to " + streamConfig.getName());
      }
    };
    return getContextTask;
  }

  private Runnable getUpdateContextTask(final StreamConfig streamConfig) {
    Runnable updateContextTask = () -> {
      long[] requestSize =
          GaussianDistribution.getRequestArray(ApplicationUtils.getGaussianTimeWindow(),
              Math.floorDiv(streamConfig.getPeakRequestSize(), 10));
      for (long requests : requestSize) {
        long avgSleepDelay = (int) ((1000 * 60 * 5) / requests);
        int numRequests = 0;
        for (int requestCount = 0; requestCount < requests; requestCount++) {
          try {
            String context = streamConfig.getName();
            List<AttributeDesc> attributes = streamConfig.getAttributes();
            Map<String, String> attrMap = new HashMap<>();
            for (int index = 1; index < attributes.size(); index++) {
              attrMap.put(attributes.get(index).getName(),
                  data.genAttrValue(attributes.get(index), streamConfig));
            }
            final String key = data.getContextkey(streamConfig.getName(), streamConfig.getName());
            ISqlResponse ctxRecords =
                DbUtils.getContextEntries(userSession, context, key);
            if (null != ctxRecords && !ctxRecords.getRecords().isEmpty()
                && ctxRecords.getRecords().get(0) != null) {
              logger.debug(
                  String.format("Updating context %s for key %s with %d attr : %s : %s",
                      streamConfig.getName(), key, attributes.size(), attrMap.keySet(),
                      attrMap.values()));
              ++numRequests;
              DbUtils.updateContextEntry(userSession, context, key, attrMap);
            } else {
              logger.debug(String.format("No event found for %s in context %s", key,
                  streamConfig.getName()));
            }
            TimeUnit.MILLISECONDS.sleep(avgSleepDelay);
          } catch (BiosClientException e) {
            handleBiosClientError(e);
          } catch (ContextKeyNotFoundException cne) {
            logger.debug(cne.getMessage());
          } catch (Exception e) {
            logger.error(e.getMessage(), e);
          }
        }
        logger
            .info("PUBLISHED " + numRequests + " UPDATE CONTEXT requests to "
                + streamConfig.getName());
      }
    };
    return updateContextTask;
  }

  public Runnable getExtractExecutorThreads(final StreamConfig streamConfig) {
    Runnable extractTask = () -> {
      long[] requestSize =
          GaussianDistribution.getRequestArray(ApplicationUtils.getGaussianTimeWindow(),
              streamConfig.getPeakRequestSize());
      long avgSleepDelay = 60000L;
      for (int i = 0; i < requestSize.length; i++) {
        try {
          if (streamConfig.getType().equals(StreamType.SIGNAL)) {
            final ISqlResponse response =
                DbUtils.select(userSession, streamConfig.getName(),
                    System.currentTimeMillis(), -avgSleepDelay * 1L);
            List<Record> events = response.getDataWindows().get(0).getRecords();
            logger.info(String.format("Fetched %d records from %s stream", events.size(),
                streamConfig.getName()));
          }
          TimeUnit.MILLISECONDS.sleep(avgSleepDelay);
        } catch (BiosClientException e) {
          handleBiosClientError(e);
        } catch (Exception e) {
          logger.error(e.getMessage(), e);
        }
      }
    };
    return extractTask;
  }

  private void handleBiosClientError(BiosClientException e) {
    if ((e.getCode() != null) && e.getCode().equals(BiosClientError.SESSION_EXPIRED)) {
      try {
        logger.warn("Session failed, recreating bios session");
        String host = ApplicationUtils.getHost();
        int port = ApplicationUtils.getPort();
        DbUtils.renew(userSession, host, port, ApplicationUtils.getEmail(),
            ApplicationUtils.getPassword());
      } catch (BiosClientException e1) {
        logger.error(e);
      }
    } else if ((e.getCode() != null) && ((e.getCode().equals(BiosClientError.BAD_GATEWAY))
        || (e.getCode().equals(BiosClientError.CLIENT_CHANNEL_ERROR))
        || (e.getCode().equals(BiosClientError.GENERIC_CLIENT_ERROR))
        || (e.getCode().equals(BiosClientError.SERVER_CHANNEL_ERROR))
        || (e.getCode().equals(BiosClientError.SESSION_INACTIVE))
        || (e.getCode().equals(BiosClientError.SERVICE_UNDEPLOYED))
        || (e.getCode().equals(BiosClientError.SERVICE_UNAVAILABLE))
        || (e.getCode().equals(BiosClientError.SERVER_CONNECTION_FAILURE))
        || (e.getCode().equals(BiosClientError.NOT_FOUND))
        || (e.getCode().equals(BiosClientError.SCHEMA_VERSION_CHANGED)))) {
      try {
        TimeUnit.SECONDS.sleep(30L);
      } catch (InterruptedException ie) {
        logger.error(ie);
      }
    } else if ((e.getCode() != null) && ((e.getCode().equals(BiosClientError.BAD_INPUT))
        || (e.getCode().equals(BiosClientError.BULK_INGEST_FAILED))
        || (e.getCode().equals(BiosClientError.INVALID_REQUEST))
        || (e.getCode().equals(BiosClientError.INVALID_ARGUMENT)))) {
      logger.warn(e.getMessage(), e);
    } else {
      logger.error(e.getMessage(), e);
    }
  }

}
