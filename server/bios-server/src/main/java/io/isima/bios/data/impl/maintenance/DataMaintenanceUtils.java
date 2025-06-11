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
package io.isima.bios.data.impl.maintenance;

import static io.isima.bios.service.Keywords.POST;

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.EventFactory;
import io.isima.bios.common.ExtractState;
import io.isima.bios.dto.MultiContextEntriesSpec;
import io.isima.bios.dto.PutContextEntriesRequest;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.ExecutorManager;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.ContentRepresentation;
import io.isima.bios.models.Event;
import io.isima.bios.models.ExtractRequest;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.View;
import io.isima.bios.models.v1.Aggregate;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.server.handlers.ContextWriteOpState;
import io.isima.bios.server.services.BiosServicePath;
import io.isima.bios.service.HttpFanRouter;
import io.isima.bios.service.JwtTokenUtils;
import io.isima.bios.service.handler.FanRouter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataMaintenanceUtils {
  private static final Logger logger = LoggerFactory.getLogger(DataMaintenanceUtils.class);

  public static CompletableFuture<List<Event>> extractEventsAsync(
      StreamDesc streamDesc,
      long start,
      long end,
      List<String> attributes,
      List<Aggregate> aggregates,
      View view,
      String caller,
      ExecutionState state) {

    final var request = new ExtractRequest();
    request.setStartTime(start);
    request.setEndTime(end);
    request.setAttributes(attributes);
    request.setAggregates(aggregates);
    request.setView(view);

    /*
    final var extractState =
        new ExtractState(caller, state, () -> DynamicServerRecord.newEventFactory(streamDesc));
        */
    final var extractState = new ExtractState(caller, state, () -> new EventFactory() {});
    extractState.setTenantName(streamDesc.getParent().getName());
    extractState.setStreamName(streamDesc.getName());
    extractState.setInput(request);
    extractState.setStreamDesc(streamDesc);
    extractState.setIndexQueryEnabled(false);
    extractState.setRollupTask(true);
    extractState.setDataLengthLimited(false);

    final var eventsFuture = new CompletableFuture<List<Event>>();
    BiosModules.getDataEngine()
        .extractEvents(extractState, eventsFuture::complete, eventsFuture::completeExceptionally);
    return eventsFuture;
  }

  private static void extractEventsCore(
      StreamDesc streamDesc,
      long start,
      long end,
      List<String> attributes,
      List<Aggregate> aggregates,
      View view,
      String caller,
      List<Event> eventList,
      ExecutionState maintenanceState)
      throws TfosException, ApplicationException {
    ExtractRequest request = new ExtractRequest();
    request.setStartTime(start);
    request.setEndTime(end);
    request.setAttributes(attributes);
    request.setAggregates(aggregates);
    request.setView(view);

    final var state = new ExtractState(caller, maintenanceState, () -> new EventFactory() {});
    state.setTenantName(streamDesc.getParent().getName());
    state.setStreamName(streamDesc.getName());
    state.setInput(request);
    state.setStreamDesc(streamDesc);
    state.setIndexQueryEnabled(false);
    state.setRollupTask(true);

    final var future = new CompletableFuture<List<Event>>();

    BiosModules.getDataEngine()
        .extractEvents(
            state,
            (events) -> {
              state.addHistory("}");
              logger.trace("{} was done successfully.\n{}", state.getExecutionName(), state);
              if (events == null) {
                logger.warn("extractEvents from Stream {} - EVENTS ARE NULL", streamDesc.getName());
                future.complete(List.of());
              } else {
                future.complete(events);
              }
            },
            future::completeExceptionally);

    try {
      eventList.addAll(future.get());
    } catch (ExecutionException e) {
      final var cause = e.getCause();
      if (cause instanceof TfosException) {
        final var te = (TfosException) cause;
        throw new TfosException(te);
      }
      throw new ApplicationException("Event extraction failed", cause);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ApplicationException("Event extraction failed", e);
    }
  }

  /**
   * Converts an event to CSV.
   *
   * @param event Input event.
   * @param streamDesc Target stream configuration.
   * @return Built CSV string.
   */
  public static String toCsv(Event event, StreamDesc streamDesc) {
    final var sb = new StringBuilder();
    String delimiter = "";
    for (var attribute : streamDesc.getAttributes()) {
      sb.append(delimiter);
      sb.append('"');
      if (attribute.getAttributeType() == InternalAttributeType.BLOB) {
        final var buffer = (ByteBuffer) event.get(attribute.getName());
        sb.append(Base64.getEncoder().encodeToString(buffer.array()));
      } else {
        sb.append(event.get(attribute.getName()).toString().replace("\"", "\"\""));
      }
      sb.append('"');
      delimiter = ",";
    }
    return sb.toString();
  }

  public static CompletableFuture<Void> saveContextAsync(
      String entitiesName,
      StreamDesc contextDesc,
      List<String> csvEntries,
      long putEntriesTimestamp,
      ExecutionState parentState) {
    final var putEntriesRequest = new PutContextEntriesRequest();
    putEntriesRequest.setEntries(csvEntries);
    putEntriesRequest.setContentRepresentation(ContentRepresentation.CSV);

    final var tenantName = contextDesc.getParent().getName();
    final var contextName = contextDesc.getName();

    final var state =
        new ContextWriteOpState<PutContextEntriesRequest, List<Event>>(
            "Put " + entitiesName, parentState.getExecutor());
    state.setInitialParams(
        tenantName,
        contextName,
        null,
        putEntriesRequest,
        RequestPhase.INITIAL,
        putEntriesTimestamp);
    state.setAtomicOperationContext(parentState.getAtomicOperationContext());

    final long putRequestTime = System.currentTimeMillis();
    final var sessionToken = JwtTokenUtils.makeTemporarySessionToken(tenantName, putRequestTime);
    final FanRouter<PutContextEntriesRequest, Void> fanRouter =
        new HttpFanRouter<>(
            POST,
            BiosServicePath.ROOT,
            () -> String.format("/tenants/%s/contexts/%s/entries", tenantName, contextName),
            sessionToken.getToken(),
            BiosModules.getHttpClientManager(),
            Void.class);

    final var handler = BiosModules.getDataServiceHandler();
    return handler.upsertContextEntries(sessionToken, null, state, fanRouter);
  }

  public static CompletableFuture<Void> deleteContextEntriesAsync(
      final String tenantName,
      final String contextName,
      final long now,
      final ArrayList<List<Object>> keysToDelete,
      ExecutionState parentState) {

    final var sessionToken = JwtTokenUtils.makeTemporarySessionToken(tenantName, now);

    final var deleteSpec = new MultiContextEntriesSpec();
    deleteSpec.setPrimaryKeys(keysToDelete);
    deleteSpec.setContentRepresentation(ContentRepresentation.UNTYPED);
    final FanRouter<MultiContextEntriesSpec, Void> fanRouter =
        new HttpFanRouter<>(
            POST,
            BiosServicePath.ROOT,
            () -> String.format("/tenants/%s/contexts/%s/entries/delete", tenantName, contextName),
            sessionToken.getToken(),
            BiosModules.getHttpClientManager(),
            Void.class);

    final var state =
        new ContextWriteOpState("DeleteContextEntriesForMaintenance", parentState.getExecutor());
    parentState.addBranch(state);
    state.setInitialParams(tenantName, contextName, null, deleteSpec, RequestPhase.INITIAL, now);

    final var handler = BiosModules.getDataServiceHandler();

    return handler.deleteContextEntries(sessionToken, null, state, fanRouter);
  }

  @Deprecated
  public static void deleteContextEntriesWithFanRouter(
      final String tenantName,
      final String contextName,
      final long now,
      final ArrayList<List<Object>> keysToDelete)
      throws ApplicationException, TfosException {

    final var sessionToken = JwtTokenUtils.makeTemporarySessionToken(tenantName, now);

    final var deleteSpec = new MultiContextEntriesSpec();
    deleteSpec.setPrimaryKeys(keysToDelete);
    deleteSpec.setContentRepresentation(ContentRepresentation.UNTYPED);
    final FanRouter<MultiContextEntriesSpec, Void> fanRouter =
        new HttpFanRouter<>(
            POST,
            BiosServicePath.ROOT,
            () -> String.format("/tenants/%s/contexts/%s/entries/delete", tenantName, contextName),
            sessionToken.getToken(),
            BiosModules.getHttpClientManager(),
            Void.class);

    final var state =
        new ContextWriteOpState(
            "DeleteContextEntriesForMaintenance", ExecutorManager.getSidelineExecutor());
    state.setInitialParams(tenantName, contextName, null, deleteSpec, RequestPhase.INITIAL, now);

    final var handler = BiosModules.getDataServiceHandler();

    waitForCompletion(
        handler.deleteContextEntries(sessionToken, null, state, fanRouter),
        "deleting context entries");
  }

  @Deprecated
  public static void waitForCompletion(CompletableFuture<Void> future, String operationName)
      throws ApplicationException, TfosException {
    try {
      future.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ApplicationException(operationName + " interrupted", e);
    } catch (ExecutionException e) {
      if ((e.getCause() instanceof TfosException)) {
        throw (TfosException) e.getCause();
      }
      if ((e.getCause() instanceof ApplicationException)) {
        throw (ApplicationException) e.getCause();
      }
      throw new ApplicationException(operationName + " failed", e.getCause());
    }
  }

  public static CompletableFuture<Boolean> isRollupDebugLogEnabled(
      StreamDesc signalDesc, ExecutionState state) {
    return BiosModules.getSharedProperties()
        .getPropertyAsync("prop.rollupDebugSignal", state)
        .thenApplyAsync(
            (debugSignalName) -> signalDesc.getName().equalsIgnoreCase(debugSignalName),
            state.getExecutor());
  }

  private static int compare(Event a, Event b) {
    return a.getIngestTimestamp().compareTo(b.getIngestTimestamp());
  }

  /**
   * Returns a sublist of the given events which include only ones that are within the specified
   * time range.
   *
   * <p>The source events must be sorted by timestamp.
   *
   * @param events Original events sorted by timestamp
   * @param startTime The start time of the output events (inclusive)
   * @param endTime The end time of the output events (exclusive)
   */
  public static List<Event> getEventsInRange(List<Event> events, long startTime, long endTime) {
    if (events.isEmpty()) {
      return events;
    }
    // all events are in the range in most of the cases
    if (startTime <= events.get(0).getIngestTimestamp().getTime()
        && endTime > events.get(events.size() - 1).getIngestTimestamp().getTime()) {
      return events;
    }
    int left = 0;
    int right = events.size();
    int pos = (left + right) / 2;
    while (left < right) {
      final var current = events.get(pos);
      if (current.getIngestTimestamp().getTime() < startTime) {
        left = pos + 1;
      } else {
        right = pos;
      }
      pos = (left + right) / 2;
    }
    final int indexFrom = left;

    left = 0;
    right = events.size();
    pos = (right + left) / 2;
    while (left < right) {
      final var current = events.get(pos);
      if (current.getIngestTimestamp().getTime() < endTime) {
        left = pos + 1;
      } else {
        right = pos;
      }
      pos = (left + right) / 2;
    }
    final int indexTo = right;
    return events.subList(indexFrom, indexTo);
  }

  /**
   * Returns a sublist of the given events which include only ones that are within the specified
   * time range.
   *
   * <p>The source events must be sorted by timestamp.
   *
   * @param events Original events sorted by timestamp
   * @param specifier Digest specifier, the method uses startTime and endTime for the time range
   */
  public static List<Event> getEventsInRange(List<Event> events, DigestSpecifier specifier) {
    return getEventsInRange(events, specifier.getStartTime(), specifier.getEndTime());
  }
}
