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
package io.isima.bios.data.distribution;

import com.strobel.assembler.Collection;
import io.isima.bios.data.service.Config;
import io.isima.bios.data.service.DataStore;
import io.isima.bios.load.model.AttributeDesc;
import io.isima.bios.load.model.StreamConfig;
import io.isima.bios.load.model.StreamType;
import io.isima.bios.load.utils.ApplicationUtils;
import io.isima.bios.load.utils.BiosRequestType;
import io.isima.bios.load.utils.DbUtils;
import io.isima.bios.models.DataWindow;
import io.isima.bios.models.Record;
import io.isima.bios.models.isql.ISqlResponse;
import io.isima.bios.sdk.Bios;
import io.isima.bios.sdk.Session;
import io.isima.bios.sdk.Statement;
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

public class CloneDistribution {

  private static Logger logger = Logger.getLogger(CloneDistribution.class);
  private Config config;
  private DataStore data;
  private Session adminSession;
  private Session userSession;

  public CloneDistribution(final Session userSession, final Session adminSession,
      final Config config, final DataStore data) {
    this.config = config;
    this.data = data;
    this.userSession = userSession;
    this.adminSession = adminSession;
  }

  public ScheduledExecutorService initExecutors(int noThreads) {
    return new ScheduledThreadPoolExecutor(noThreads);
  }

  public Collection<Runnable> getDistributionTasks() {
    Collection<Runnable> loadTasks = new Collection<>();
    Set<String> streams = config.getStreamNames();
    for (String stream : streams) {
      StreamConfig streamConfig = config.getStream(stream);
      if (streamConfig.getType().equals(StreamType.CONTEXT)) {
        loadTasks.add(getPutContextTasks(streamConfig));
        loadTasks.add(getContextItemTask(streamConfig));
//        loadTasks.add(getUpdateContextTask(streamConfig));
      } else {
        loadTasks.addAll(getIngestExtractTasks(streamConfig));
      }
    }
    return loadTasks;
  }

  private Collection<Runnable> getIngestExtractTasks(final StreamConfig streamConfig) {
    String stream = streamConfig.getName();
    Collection<Runnable> ingestExtractTasks = new Collection<>();
    Runnable ingestTask = () -> {
      long[] requestSize;
      try {
        requestSize = getRequestSize(streamConfig.getName(), BiosRequestType.INSERT.name());
        for (int i = 0; i < requestSize.length; i++) {
          long avgSleepDelay = (int) ((1000 * 60 * 5) / requestSize[i]);
          for (int requestCount = 0; requestCount < requestSize[i]; requestCount++) {
            ISqlResponse response =
                DbUtils.insert(userSession, streamConfig.getName(), data.generateEvent(stream));
            TimeUnit.MILLISECONDS.sleep(avgSleepDelay);
          }
          logger.info("PUBLISHED " + requestSize[i] + " INSERT events to " + stream);
        }
      } catch (BiosClientException e) {
        handleBiosClientError(e);
      } catch (Exception e) {
        logger.error(e.getMessage());
      }
    };
    ingestExtractTasks.add(ingestTask);
    ingestExtractTasks.add(getExtractExecutorThreads(streamConfig));
    return ingestExtractTasks;
  }

  private Runnable getPutContextTasks(final StreamConfig streamConfig) {
    Runnable contextTask = () -> {
      try {
        long[] requestSize =
            getRequestSize(streamConfig.getName(), BiosRequestType.PUT_CONTEXT_ENTRIES.name());
        for (int i = 0; i < requestSize.length; i++) {
          long avgSleepDelay = (int) ((1000 * 60 * 5) / requestSize[i]);
          for (int requestCount = 0; requestCount < requestSize[i]; requestCount++) {
            final String event = data.generateEvent(streamConfig.getName());
            DbUtils.putContextEntries(userSession, streamConfig.getName(), Arrays.asList(event));
            TimeUnit.MILLISECONDS.sleep(avgSleepDelay);
          }
          logger.info(
              "PUBLISHED " + requestSize[i] + " PUT CONTEXT events to " + streamConfig.getName());
        }
      } catch (BiosClientException e) {
        handleBiosClientError(e);
      } catch (Exception e) {
        logger.error(e.getMessage());
      }
    };

    return contextTask;
  }

  private Runnable getContextItemTask(final StreamConfig streamConfig) {
    Runnable getContextTask = () -> {
      try {
        long[] requestSize =
            getRequestSize(streamConfig.getName(), BiosRequestType.GET_CONTEXT.name());
        for (long requests : requestSize) {
          long avgSleepDelay = (int) ((1000 * 60 * 5) / requests);
          for (int requestCount = 0; requestCount < requests; requestCount++) {
            String context = streamConfig.getName();
            final String key = data.getContextkey(streamConfig.getAttributes().get(0).getName(),
                streamConfig.getName());
            DbUtils.getContextEntries(userSession, context, key);
            TimeUnit.MILLISECONDS.sleep(avgSleepDelay);

          }
          logger.info("PUBLISHED " + requests + " GET CONTEXT events to " + streamConfig.getName());
        }
      } catch (BiosClientException e) {
        handleBiosClientError(e);
      } catch (Exception e) {
        logger.error(e.getMessage());
      }
    };
    return getContextTask;
  }

  private Runnable getUpdateContextTask(final StreamConfig streamConfig) {
    Runnable updateContextTask = () -> {
      try {
        long[] requestSize =
            getRequestSize(streamConfig.getName(), BiosRequestType.UPDATE_CONTEXT.name());
        for (long requests : requestSize) {
          long avgSleepDelay = (int) ((1000 * 60 * 5) / requests);
          for (int requestCount = 0; requestCount < requests; requestCount++) {
            String context = streamConfig.getName();
            List<AttributeDesc> attributes = streamConfig.getAttributes();
            Map<String, String> attrMap = new HashMap<>();
            for (int index = 1; index < attributes.size(); index++) {
              attrMap.put(attributes.get(index).getName(),
                  data.genAttrValue(attributes.get(index), streamConfig));
            }
            final String key = data.getContextkey(streamConfig.getName(), streamConfig.getName());
            ISqlResponse response =
                DbUtils.getContextEntries(userSession, context, key);
            if (!response.getRecords().isEmpty() && response.getRecords().get(0) != null) {
              logger.debug(
                  String.format("Updating context %s for key %s with %d attr : %s : %s",
                      streamConfig.getName(), key, attributes.size(), attrMap.keySet(),
                      attrMap.values()));
              DbUtils.updateContextEntry(userSession, context, key, attrMap);
            } else {
              logger.debug(String.format("No event found for %s in context %s", key,
                  streamConfig.getName()));
            }
            TimeUnit.MILLISECONDS.sleep(avgSleepDelay);
          }
          logger
              .info(
                  "PUBLISHED " + requests + " UPDATE CONTEXT events to " + streamConfig.getName());
        }
      } catch (BiosClientException e) {
        handleBiosClientError(e);
      } catch (Exception e) {
        logger.error(e.getMessage());
      }
    };
    return updateContextTask;
  }

  public Runnable getExtractExecutorThreads(final StreamConfig streamConfig) {
    Runnable extractTask = () -> {
      try {
        long[] requestSize = getRequestSize(streamConfig.getName(), BiosRequestType.SELECT.name());
        long avgSleepDelay = 60000L;
        for (int i = 0; i < requestSize.length; i++) {
          try {
            if (streamConfig.getType().equals(StreamType.SIGNAL)) {
              final ISqlResponse response = DbUtils.select(userSession,
                  streamConfig.getName(), System.currentTimeMillis(), -avgSleepDelay * 1L);
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
      } catch (BiosClientException bce) {
        handleBiosClientError(bce);
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
        userSession = DbUtils.renew(userSession, host, port, ApplicationUtils.getEmail(),
            ApplicationUtils.getPassword());
        adminSession =
            DbUtils.renew(adminSession, host, port, ApplicationUtils.getSystemAdminUser(),
                ApplicationUtils.getSystemAdminPassword());
      } catch (BiosClientException e1) {
        logger.error(e.getMessage());
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
        logger.error(ie.getMessage());
      }
    } else if ((e.getCode() != null) && ((e.getCode().equals(BiosClientError.BAD_INPUT))
        || (e.getCode().equals(BiosClientError.BULK_INGEST_FAILED))
        || (e.getCode().equals(BiosClientError.INVALID_REQUEST))
        || (e.getCode().equals(BiosClientError.INVALID_ARGUMENT)))) {
      logger.warn(e.getMessage());
    } else {
      logger.error(e.getMessage());
    }
  }

  private synchronized long[] getRequestSize(String streamName, String request)
      throws BiosClientException {
    final long window = -1000 * 60 * 5;
    long startTime = System.currentTimeMillis();
    startTime = Math.round((double) startTime / (5 * 60 * 1000)) * (5 * 60 * 1000);

    final String REQUEST_SIGNAL = "_requests";
    final long[] size = {1};
    Statement selectStatement =
        Bios.isql().select().fromSignal(REQUEST_SIGNAL)
            .where("tenant='" + ApplicationUtils.getTenantToClone() + "'")
            .timeRange(startTime, window)
            .build();
    List<DataWindow> dataWindows = adminSession.execute(selectStatement).getDataWindows();
    if (null != dataWindows && !dataWindows.isEmpty()) {
      DataWindow firstWindow = dataWindows.get(0);
      long sumCount = 1;
      for (Record record : firstWindow.getRecords()) {
        String stream = record.getAttribute("stream").asString();
        String tenant = record.getAttribute("tenant").asString();
        if (tenant.equalsIgnoreCase(ApplicationUtils.getTenantToClone())
            && stream.equalsIgnoreCase(streamName)
            && (record.getAttribute("request").asString().equalsIgnoreCase(request)
                || record.getAttribute("request").asString().equalsIgnoreCase("INSERT_BULK"))) {
          if (record.getAttribute("request").asString().equalsIgnoreCase("INSERT_BULK")) {
            sumCount += record.getAttribute("subReqSuccessCount").asLong();
          } else {
            sumCount += record.getAttribute("reqSuccessCount").asLong();
          }
        }
      }
      long loadRequests =
          Math.max(Math.round(sumCount * ((double) ApplicationUtils.getLoadPercent() / 100)), 1L);
      size[0] = loadRequests;
      logger.info(
          String.format(
              "Found tenant:%s, stream:%s, request:%s, recordSize: %d, loadSize:%d, startTime:%d",
              ApplicationUtils.getTenantToClone(), streamName, request, sumCount, loadRequests,
              startTime));

    }
    logger.warn(
        String.format(
            "Not Found tenant:%s, stream:%s, request:%s, modify load.properties with other load pattern",
            ApplicationUtils.getTenantToClone(), streamName, request));
    return size;
  }
}
