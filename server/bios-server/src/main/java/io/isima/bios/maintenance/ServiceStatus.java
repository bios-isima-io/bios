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
package io.isima.bios.maintenance;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.isima.bios.admin.v1.SharedConfig;
import io.isima.bios.common.BiosConstants;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.server.services.RequestStream;
import io.isima.bios.utils.BiosObjectMapperProvider;
import io.isima.bios.utils.Utils;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceStatus {
  private static final long INFLIGHT_OPERATION_WAIT_TIMEOUT_MILLIS = 30000;
  private static Logger logger = LoggerFactory.getLogger(ServiceStatus.class);

  private final SharedConfig sharedConfig;

  @Getter private final String myNodeName;
  private String myEndpoint;

  @Getter @Setter private boolean inMaintenance;

  @Getter private final Set<RequestStream> inFlightOperations;

  public ServiceStatus(SharedConfig sharedConfig) throws ApplicationException {
    this(sharedConfig, null);
  }

  public ServiceStatus(SharedConfig sharedConfig, String myNodeName) throws ApplicationException {
    this.myNodeName = myNodeName == null ? Utils.getNodeName() : myNodeName;
    this.sharedConfig = sharedConfig;
    this.myEndpoint = null;
    this.inMaintenance = false;
    this.inFlightOperations = ConcurrentHashMap.newKeySet();
  }

  public void initialize() {
    getMyEndpoint();
  }

  public synchronized void enterMaintenanceMode() {
    if (inMaintenance) {
      return;
    }
    logger.info("Entering maintenance mode");
    try {
      addFailedEndpoint();
      inMaintenance = true;
    } catch (ApplicationException e) {
      logger.error("Failed to mark the service as unavailable", e);
    }
  }

  public synchronized void clearMaintenanceMode() throws ApplicationException {
    logger.info("Exiting maintenance mode");
    inMaintenance = false;
  }

  public String getMyEndpoint() {
    if (myEndpoint == null) {
      for (String ep : sharedConfig.getEndpoints()) {
        final String endpoint =
            ep.startsWith(BiosConstants.PREFIX_FAILED_NODE_NAME) ? ep.substring(1) : ep;
        final String node = sharedConfig.getNodeName(endpoint);
        if (myNodeName.equals(node)) {
          myEndpoint = endpoint;
          break;
        }
      }
    }
    return myEndpoint;
  }

  private void addFailedEndpoint() throws ApplicationException {
    if (getMyEndpoint() == null) {
      return;
    }
    sharedConfig.addFailedEndpoint(myEndpoint);
  }

  public void removeFailedEndpoint() throws ApplicationException {
    if (getMyEndpoint() == null) {
      return;
    }
    sharedConfig.removeFailedEndpoint(myEndpoint);
  }

  public void waitForInFlightOperationsComplete() {
    logger.info("Waiting for in-flight operations completed");
    long sleepTime = 1000;
    final long start = System.currentTimeMillis();
    boolean isFirst = true;
    while (!inFlightOperations.isEmpty()) {
      if (System.currentTimeMillis() - start > INFLIGHT_OPERATION_WAIT_TIMEOUT_MILLIS) {
        logger.warn("Timeout to flush in-flight operations");
        break;
      }
      int numRemaining = inFlightOperations.size();
      final var objectMapper = BiosObjectMapperProvider.get();
      final var opInfo = new ArrayList<String>();
      logger.info("{} in-flight operations are remaining", numRemaining);
      if (isFirst) {
        isFirst = false;
        for (var inFlightOperation : inFlightOperations) {
          final var peerAddress = inFlightOperation.getPeerAddress();
          final var operationName = inFlightOperation.getHandler().getOperationName();
          final var state = inFlightOperation.getRootState();
          final String trace = state != null ? state.getCallTraceString() : "";
          String user = "";
          String appType = "";
          String appName = "";
          final var userContext = state != null ? state.getUserContext() : null;
          if (userContext != null) {
            user = userContext.getSubject();
            appType = userContext.getAppType() != null ? userContext.getAppType().stringify() : "";
            appName = userContext.getAppName() != null ? userContext.getAppName() : "";
          }
          logger.info(
              "  operation={}, peer={}, user={}, appType={}, appName={}, trace={}",
              operationName,
              peerAddress,
              user,
              appType,
              appName,
              trace);
          try {
            opInfo.add(
                objectMapper.writeValueAsString(
                    Map.of(
                        "peer", peerAddress.toString(),
                        "operation", operationName,
                        "user", user,
                        "appType", appType,
                        "appName", appName)));
          } catch (JsonProcessingException e) {
            logger.error("Some error in building in-flight operation info", e);
          }
        }
      }
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
        logger.warn("Interrupted");
        Thread.currentThread().interrupt();
      }
    }
    logger.info("In-flight operations completed");
  }
}
