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
package io.isima.bios.it.tools;

import static io.isima.bios.service.Keywords.POST;
import static java.lang.Thread.sleep;

import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.auth.Auth;
import io.isima.bios.common.ActivityRecorder;
import io.isima.bios.data.ColumnDefinition;
import io.isima.bios.data.DataEngine;
import io.isima.bios.data.DefaultRecord;
import io.isima.bios.data.impl.DataUtils;
import io.isima.bios.dto.MultiContextEntriesSpec;
import io.isima.bios.dto.PutContextEntriesRequest;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ContextOpState;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.ExecutorManager;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.models.AppType;
import io.isima.bios.models.BiosVersion;
import io.isima.bios.models.ContentRepresentation;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.models.ExtractRequest;
import io.isima.bios.models.InsertResponseRecord;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.SelectResponseRecords;
import io.isima.bios.models.SessionToken;
import io.isima.bios.models.SummarizeRequest;
import io.isima.bios.models.UserContext;
import io.isima.bios.models.isql.Metric;
import io.isima.bios.models.isql.OrderBy;
import io.isima.bios.models.isql.ResponseShape;
import io.isima.bios.models.isql.SelectStatement;
import io.isima.bios.models.isql.Window;
import io.isima.bios.models.proto.DataProto;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.Permission;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.server.handlers.ContextWriteOpState;
import io.isima.bios.server.handlers.DataServiceHandler;
import io.isima.bios.server.handlers.InsertState;
import io.isima.bios.server.handlers.MultiSelectState;
import io.isima.bios.server.handlers.SelectState;
import io.isima.bios.server.services.BiosServicePath;
import io.isima.bios.service.HttpClientManager;
import io.isima.bios.service.HttpFanRouter;
import io.isima.bios.service.JwtTokenUtils;
import io.isima.bios.service.handler.FanRouter;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class TestUtils {

  public static ObjectMapper mapper = TfosObjectMapperProvider.get();
  public static ExecutorService service = Executors.newSingleThreadExecutor();

  public static final String CONTEXT_GEO_LOCATION_SRC =
      "{"
          + "  'name': 'geo_location',"
          + "  'type': 'context',"
          + "  'attributes': ["
          + "    {'name': 'ip', 'type': 'string'},"
          + "    {'name': 'country', 'type': 'string'},"
          + "    {'name': 'state', 'type': 'string'}"
          + "  ]"
          + "}";

  public static final String CONTEXT_GEO_LOCATION_MD_SRC =
      "{"
          + "  'contextName': 'geoLocation',"
          + "  'missingAttributePolicy': 'Reject',"
          + "  'attributes': ["
          + "    {'attributeName': 'ipVersion', 'type': 'string'},"
          + "    {'attributeName': 'ip', 'type': 'string'},"
          + "    {'attributeName': 'country', 'type': 'string'},"
          + "    {'attributeName': 'state', 'type': 'string'}"
          + "  ],"
          + "  'primaryKey': ['ipVersion', 'ip']"
          + "}";

  public static final String CONTEXT_ALL_TYPES_SRC =
      "{"
          + "  'name': 'ctx_all_types',"
          + "  'type': 'context',"
          + "  'missingValuePolicy': 'strict',"
          + "  'attributes': ["
          + "    {'name': 'intAttribute', 'type': 'long'},"
          + "    {'name': 'intAttribute1', 'type': 'long'},"
          + "    {'name': 'stringAttribute', 'type': 'string'},"
          + "    {'name': 'numberAttribute', 'type': 'number'},"
          + "    {'name': 'numberAttribute1', 'type': 'number'},"
          + "    {'name': 'doubleAttribute', 'type': 'double'},"
          + "    {'name': 'doubleAttribute1', 'type': 'double'},"
          + "    {'name': 'inetAttribute', 'type': 'inet'},"
          + "    {'name': 'dateAttribute', 'type': 'date'}, "
          + "    {'name': 'timestampAttribute', 'type': 'timestamp'},"
          + "    {'name': 'uuidAttribute', 'type': 'uuid'},"
          + "    {'name': 'booleanAttribute', 'type': 'boolean'},"
          + "    {'name': 'enumAttribute', 'type': 'enum',"
          + "     'enum': ["
          + "       'enum_const_1', 'enum_const_2','enum_const_3','enum_const_4'"
          + "     ]},"
          + "    {'name': 'blobAttribute', 'type': 'blob'}"
          + "  ]"
          + "}";

  /**
   * Method used for reverting to saved property.
   *
   * <p>The method sets the property value if it is non-null, otherwise clears the property.
   *
   * @param key Property key to revert.
   * @param value The value to revert back. The value can be null.
   */
  public static void revertProperty(String key, String value) {
    if (value != null) {
      System.setProperty(key, value);
    } else {
      System.clearProperty(key);
    }
  }

  public static InsertResponseRecord insert(
      DataServiceHandler dataServiceHandler, String tenantName, String signalName, String csvText)
      throws ApplicationException, TfosException {
    return insert(dataServiceHandler, tenantName, signalName, UUIDs.timeBased(), csvText);
  }

  public static InsertResponseRecord insert(
      DataServiceHandler dataServiceHandler,
      String tenantName,
      StreamConfig signal,
      long timestamp,
      String csvText)
      throws ApplicationException, TfosException {
    return insert(
        dataServiceHandler, tenantName, signal.getName(), UUIDs.startOf(timestamp), csvText);
  }

  public static InsertResponseRecord insert(
      DataServiceHandler dataServiceHandler,
      String tenantName,
      String signalName,
      UUID eventId,
      String csvText)
      throws ApplicationException, TfosException {
    final var tempToken =
        JwtTokenUtils.makeTemporarySessionToken(tenantName, System.currentTimeMillis());

    final var state = new InsertState(service, dataServiceHandler.getDataEngine());
    final var request =
        DataProto.InsertRequest.newBuilder()
            .setContentRep(DataProto.ContentRepresentation.CSV)
            .setRecord(DataProto.Record.newBuilder().addStringValues(csvText).build())
            .build();
    state.setInitialParams(tenantName, signalName, eventId, request);
    return DataUtils.wait(dataServiceHandler.insertSignal(tempToken, null, state), "insert");
  }

  public static void upsertContextEntries(
      DataServiceHandler dataServiceHandler,
      HttpClientManager clientManager,
      AdminInternal admin,
      String tenantName,
      String contextName,
      List<Event> entries)
      throws TfosException, ApplicationException {
    final var context = admin.getStream(tenantName, contextName);
    final var csvTexts = new ArrayList<String>();
    for (var entry : entries) {
      final var csv = new StringJoiner(",");
      for (var attributeDesc : context.getAttributes()) {
        final Object value = entry.get(attributeDesc.getName());
        if (attributeDesc.getAttributeType() == InternalAttributeType.STRING) {
          csv.add("\"" + value.toString().replace("\"", "\"\"") + "\"");
        } else {
          csv.add(value.toString());
        }
      }
      csvTexts.add(csv.toString());
    }
    upsertContextEntries(dataServiceHandler, clientManager, tenantName, contextName, csvTexts);
  }

  public static void upsertContextEntries(
      DataServiceHandler dataServiceHandler,
      HttpClientManager clientManager,
      String tenantName,
      String contextName,
      List<String> entries)
      throws ApplicationException, TfosException {
    upsertContextEntries(dataServiceHandler, clientManager, tenantName, contextName, entries, null);
  }

  public static void upsertContextEntries(
      DataServiceHandler dataServiceHandler,
      HttpClientManager clientManager,
      String tenantName,
      String contextName,
      List<String> entries,
      ActivityRecorder activityRecorder)
      throws ApplicationException, TfosException {

    final var tempToken =
        JwtTokenUtils.makeTemporarySessionToken(tenantName, System.currentTimeMillis());

    final Long timestamp = System.currentTimeMillis();
    final var phase = RequestPhase.INITIAL;

    final FanRouter<PutContextEntriesRequest, Void> fanRouter =
        new HttpFanRouter<>(
            POST,
            BiosServicePath.ROOT,
            () -> String.format("/tenants/%s/contexts/%s/entries", tenantName, contextName),
            tempToken.getToken(),
            clientManager,
            Void.class);

    final var state =
        new ContextWriteOpState<PutContextEntriesRequest, List<Event>>(
            "TestUpsert", ExecutorManager.getSidelineExecutor());
    state.setActivityRecorder(activityRecorder);

    final var request = new PutContextEntriesRequest();
    request.setEntries(entries);
    request.setContentRepresentation(ContentRepresentation.CSV);

    state.setInitialParams(tenantName, contextName, null, request, phase, timestamp);
    DataUtils.wait(
        dataServiceHandler.upsertContextEntries(tempToken, null, state, fanRouter), "upsert");
  }

  public static List<Event> getContextEntriesSingleKeys(
      DataServiceHandler dataServiceHandler,
      String tenantName,
      String contextName,
      List<Object> singleKeys)
      throws ApplicationException, TfosException {
    return getContextEntries(
        dataServiceHandler,
        tenantName,
        contextName,
        singleKeys.stream().map((key) -> List.of(key)).collect(Collectors.toList()));
  }

  public static List<Event> getContextEntries(
      DataServiceHandler dataServiceHandler,
      String tenantName,
      String contextName,
      List<List<Object>> keys)
      throws ApplicationException, TfosException {
    final var tempToken =
        JwtTokenUtils.makeTemporarySessionToken(tenantName, System.currentTimeMillis());
    final var request = new MultiContextEntriesSpec();
    request.setPrimaryKeys(keys);
    request.setContentRepresentation(ContentRepresentation.UNTYPED);
    final var state =
        new ContextOpState("TestGetContextEntries", ExecutorManager.getSidelineExecutor());
    state.setTenantName(tenantName);
    state.setStreamName(contextName);
    final var future = dataServiceHandler.getContextEntries(tempToken, request, state);
    return DataUtils.wait(
        future.thenApply(
            (response) ->
                response.getEntries().stream()
                    .map(
                        (record) -> {
                          final Event event = new EventJson();
                          for (int i = 0; i < response.getDefinitions().size(); ++i) {
                            final var attr = response.getDefinitions().get(i);
                            final var value = record.getAttributes().get(i);
                            event.set(attr.getName(), value);
                          }
                          return event;
                        })
                    .collect(Collectors.toList())),
        "getContextEntries");
  }

  public static List<List<Object>> listContextPrimaryKeys(
      AdminInternal admin, DataEngine dataEngine, String tenantName, String contextName)
      throws TfosException, ApplicationException {
    final var contextDesc = admin.getStream(tenantName, contextName);
    final var state =
        new ContextOpState("TestListPrimaryKeys", Executors.newSingleThreadExecutor());
    state.setTenantName(tenantName);
    state.setStreamName(contextName);
    final var future = new CompletableFuture<List<List<Object>>>();
    dataEngine.listContextPrimaryKeysAsync(
        contextDesc, state, future::complete, future::completeExceptionally);
    return DataUtils.wait(future, "listContextPrimaryKeys");
  }

  public static void deleteContextEntries(
      DataServiceHandler dataServiceHandler,
      HttpClientManager clientManager,
      String tenantName,
      String contextName,
      List<List<Object>> keys)
      throws ApplicationException, TfosException {
    deleteContextEntries(dataServiceHandler, clientManager, tenantName, contextName, keys, null);
  }

  public static void deleteContextEntries(
      DataServiceHandler dataServiceHandler,
      HttpClientManager clientManager,
      String tenantName,
      String contextName,
      List<List<Object>> keys,
      ActivityRecorder activityRecorder)
      throws ApplicationException, TfosException {

    final var tempToken =
        JwtTokenUtils.makeTemporarySessionToken(tenantName, System.currentTimeMillis());

    final Long timestamp = System.currentTimeMillis();
    final var phase = RequestPhase.INITIAL;

    final FanRouter<MultiContextEntriesSpec, Void> fanRouter =
        new HttpFanRouter<>(
            POST,
            BiosServicePath.ROOT,
            () -> String.format("/tenants/%s/contexts/%s/entries/delete", tenantName, contextName),
            tempToken.getToken(),
            clientManager,
            Void.class);

    final var state =
        new ContextWriteOpState<MultiContextEntriesSpec, List<List<Object>>>(
            "TestDeleteContextEntries", ExecutorManager.getSidelineExecutor());
    state.setActivityRecorder(activityRecorder);

    final var request = new MultiContextEntriesSpec();
    request.setPrimaryKeys(keys);
    request.setContentRepresentation(ContentRepresentation.UNTYPED);

    state.setInitialParams(tenantName, contextName, null, request, phase, timestamp);
    DataUtils.wait(
        dataServiceHandler.deleteContextEntries(tempToken, null, state, fanRouter),
        "delete context entries");
  }

  public static List<Event> extract(
      DataServiceHandler dataServiceHandler, StreamDesc streamDesc, ExtractRequest request)
      throws Throwable {
    return extract(
        dataServiceHandler, streamDesc.getParent().getName(), streamDesc.getName(), request);
  }

  public static List<Event> extract(
      DataServiceHandler dataServiceHandler,
      String tenantName,
      String signalName,
      ExtractRequest request)
      throws ApplicationException, TfosException {

    final var tempToken =
        JwtTokenUtils.makeTemporarySessionToken(tenantName, System.currentTimeMillis(), "", -1L);

    final var queryBuilder =
        DataProto.SelectQuery.newBuilder()
            .setFrom(signalName)
            .setStartTime(request.getStartTime())
            .setEndTime(request.getEndTime());

    if (request.getAttributes() != null) {
      queryBuilder.setAttributes(
          DataProto.AttributeList.newBuilder().addAllAttributes(request.getAttributes()));
    }

    final var view = request.getView();
    if (view != null) {
      final var func = view.getFunction();
      switch (func) {
        case GROUP:
          if (view.getDimensions() != null) {
            final var dimensions =
                DataProto.Dimensions.newBuilder().addAllDimensions(view.getDimensions()).build();
            queryBuilder.setGroupBy(dimensions);
          }
          break;
        case SORT:
          {
            final var orderBy = DataProto.OrderBy.newBuilder().setBy(view.getBy());
            if (view.getCaseSensitive() != null) {
              orderBy.setCaseSensitive(view.getCaseSensitive());
            }
            if (view.getReverse() != null) {
              orderBy.setReverse(view.getReverse());
            }
            queryBuilder.setOrderBy(orderBy);
          }
          break;
        default:
          // do nothing
      }
      if (view.getDimensions() != null) {
        final var dimensions =
            DataProto.Dimensions.newBuilder().addAllDimensions(view.getDimensions()).build();
        queryBuilder.setGroupBy(dimensions);
      }
    }

    if (request.getAggregates() != null) {
      for (var aggregate : request.getAggregates()) {
        final var metric =
            DataProto.Metric.newBuilder().setFunction(aggregate.getFunction().toProto());
        if (aggregate.getBy() != null) {
          metric.setOf(aggregate.getBy());
        }
        if (aggregate.getAs() != null) {
          metric.setAs(aggregate.getAs());
        }
        queryBuilder.addMetrics(metric);
      }
    }

    if (request.getFilter() != null) {
      queryBuilder.setWhere(request.getFilter());
    }

    final var selectRequest = DataProto.SelectRequest.newBuilder().addQueries(queryBuilder).build();

    final var state = new MultiSelectState(service);
    state.setInitialParams(tenantName, selectRequest, new BiosVersion(null));

    return DataUtils.wait(dataServiceHandler.multiSelect(tempToken, state), "extract").stream()
        .map(TestUtils::parseSimpleQueryResponse)
        .collect(Collectors.toList())
        .get(0);
  }

  public static List<Event> extractRollup(
      DataEngine dataEngine, StreamDesc signalDesc, ExtractRequest request)
      throws ExecutionException, InterruptedException {
    final var parentState = new GenericExecutionState("dummy", service);
    final var query =
        new SelectStatement() {
          @Override
          public long getStartTime() {
            return request.getStartTime();
          }

          @Override
          public long getEndTime() {
            return request.getEndTime();
          }

          @Override
          public boolean isDistinct() {
            return false;
          }

          @Override
          public List<String> getSelectAttributes() {
            return request.getAttributes();
          }

          @Override
          public List<? extends Metric> getMetrics() {
            return null;
          }

          @Override
          public String getFrom() {
            return signalDesc.getName();
          }

          @Override
          public String getWhere() {
            return request.getFilter();
          }

          @Override
          public List<String> getGroupBy() {
            return null;
          }

          @Override
          public OrderBy getOrderBy() {
            return null;
          }

          @Override
          public Window<?> getOneWindow() {
            return null;
          }

          @Override
          public <W extends Window<W>> Window<W> getOneWindow(Class<W> windowType) {
            return null;
          }

          @Override
          public boolean hasLimit() {
            return false;
          }

          @Override
          public int getLimit() {
            return 0;
          }

          @Override
          public boolean hasWindow() {
            return false;
          }

          @Override
          public boolean hasOrderBy() {
            return false;
          }

          @Override
          public boolean isOnTheFly() {
            return false;
          }

          @Override
          public ResponseShape getResponseShape() {
            return null;
          }
        };
    final var state =
        new SelectState(
            "ExtractRollup",
            signalDesc.getParent().getName(),
            query,
            0,
            new BiosVersion(null),
            parentState);
    state.setStreamDesc(signalDesc);
    return dataEngine
        .select(state)
        .thenApply((none) -> parseSimpleQueryResponse(state.getResponse()))
        .toCompletableFuture()
        .get();
  }

  public static Map<Long, List<Event>> executeSummarize(
      DataEngine dataEngine, StreamDesc signalDesc, SummarizeRequest request)
      throws ExecutionException, InterruptedException {

    final var parentState = new GenericExecutionState("dummy", service);
    final var query =
        new SelectStatement() {
          @Override
          public long getStartTime() {
            return request.getStartTime();
          }

          @Override
          public long getEndTime() {
            return request.getEndTime();
          }

          @Override
          public boolean isDistinct() {
            return false;
          }

          @Override
          public List<String> getSelectAttributes() {
            return null;
          }

          @Override
          public List<? extends Metric> getMetrics() {
            if (request.getAggregates() == null) {
              return null;
            }
            return request.getAggregates().stream()
                .map((aggregate) -> new TestMetric(aggregate))
                .collect(Collectors.toList());
          }

          @Override
          public String getFrom() {
            return signalDesc.getName();
          }

          @Override
          public String getWhere() {
            return request.getFilter();
          }

          @Override
          public List<String> getGroupBy() {
            return request.getGroup();
          }

          @Override
          public OrderBy getOrderBy() {
            /*
            if (request.getSort() == null) {
              return null;
            }
            */
            return null;
          }

          @Override
          public Window<?> getOneWindow() {
            if (request.getHorizon() != null
                && !request.getHorizon().equals(request.getInterval())) {
              return new TestSlidingWindow(request);
            }
            return new TestTumblingWindow(request);
          }

          @Override
          public <W extends Window<W>> Window<W> getOneWindow(Class<W> windowType) {
            return null;
          }

          @Override
          public boolean hasLimit() {
            return request.getLimit() != null;
          }

          @Override
          public int getLimit() {
            return hasLimit() ? request.getLimit() : 0;
          }

          @Override
          public boolean hasWindow() {
            return true;
          }

          @Override
          public boolean hasOrderBy() {
            // return request.getSort() != null;
            return false;
          }

          @Override
          public boolean isOnTheFly() {
            return request.isOnTheFly();
          }

          @Override
          public ResponseShape getResponseShape() {
            return null;
          }
        };

    final var state =
        new SelectState(
            "TestSummarize",
            signalDesc.getParent().getName(),
            query,
            0,
            new BiosVersion(null),
            parentState);
    state.setStreamDesc(signalDesc);

    return dataEngine
        .select(state)
        .thenApply((none) -> parseSummarizeResponse(state.getResponse()))
        .toCompletableFuture()
        .get();
  }

  private static List<Event> parseSimpleQueryResponse(SelectResponseRecords selectResponseRecords) {
    final List<Event> events;
    if (selectResponseRecords.getDataWindows().isEmpty()) {
      events = List.of();
    } else {
      final var records =
          selectResponseRecords.getDataWindows().entrySet().iterator().next().getValue();
      events =
          records.stream()
              .map((record) -> toEvent(record, selectResponseRecords.getDefinitions()))
              .collect(Collectors.toList());
    }
    return events;
  }

  private static Map<Long, List<Event>> parseSummarizeResponse(
      SelectResponseRecords selectResponseRecords) {
    final var result = new TreeMap<Long, List<Event>>();
    for (var entry : selectResponseRecords.getDataWindows().entrySet()) {
      final var timestamp = entry.getKey();
      final var events =
          entry.getValue().stream()
              .map((record) -> toEvent(record, selectResponseRecords.getDefinitions()))
              .collect(Collectors.toList());
      result.put(timestamp, events);
    }
    return result;
  }

  private static Event toEvent(DefaultRecord record, Map<String, ColumnDefinition> definitions) {
    final var event = new EventJson();
    if (record.getEventId() != null) {
      event.setEventId(record.getEventId());
    }
    if (record.getTimestamp() != null) {
      event.setIngestTimestamp(new Date(record.getTimestamp()));
    }
    for (var entry : definitions.entrySet()) {
      final String key = entry.getKey();
      final Object value = record.getAttribute(key).asObject();
      event.set(key, value);
    }
    return event;
  }

  /** System.sleep() can get interrupted; this method compensates for that. */
  @SuppressWarnings("BusyWait")
  public static void sleepAtLeast(long milliseconds) {
    final long beforeSleep = System.currentTimeMillis();
    while (System.currentTimeMillis() <= beforeSleep + milliseconds) {
      try {
        sleep(milliseconds); // While loop to compensate for interruptions.
      } catch (InterruptedException ignored) {
      }
    }
  }

  public static <RequestT, ReplyT> FanRouter<RequestT, ReplyT> makeDummyFanRouter() {
    return new FanRouter<>() {
      @Override
      public void addParam(String name, String value) {}

      @Override
      public List<ReplyT> fanRoute(RequestT request, Long timestamp) {
        return List.of();
      }

      @Override
      public CompletionStage<List<ReplyT>> fanRouteAsync(
          RequestT request, Long timestamp, ExecutionState state) {
        return null;
      }
    };
  }

  public static SessionToken makeTestSessionToken(
      Auth auth, String tenantName, List<Permission> permissions, AppType appType, String appName) {
    final var userContext = new UserContext();
    userContext.setTenant(tenantName);
    userContext.setScope("/tenants/" + tenantName + "/");
    userContext.addPermissions(permissions);
    userContext.setAppType(appType);
    userContext.setAppName(appName);
    final long now = System.currentTimeMillis();
    final long expiry = now + 3000000000L;
    return new SessionToken(auth.createToken(now, expiry, userContext), false);
  }

  public static GenericExecutionState makeGenericState() {
    return new GenericExecutionState("test", ExecutorManager.getSidelineExecutor());
  }

  public static ContextWriteOpState<String, List<Event>> makeUpsertState(
      AdminInternal tfosAdmin,
      String tenantName,
      String contextName,
      List<Event> entries,
      long timestamp)
      throws NoSuchTenantException, NoSuchStreamException {
    final var state =
        new ContextWriteOpState<String, List<Event>>(
            "test putting context", ExecutorManager.getSidelineExecutor());
    final var contextDesc = tfosAdmin.getStream(tenantName, contextName);
    state.setInitialParams(
        tenantName,
        contextName,
        contextDesc.getVersion(),
        "dummy",
        RequestPhase.INITIAL,
        timestamp);
    state.setContextDesc(contextDesc);

    state.setInputData(entries);
    return state;
  }
}
