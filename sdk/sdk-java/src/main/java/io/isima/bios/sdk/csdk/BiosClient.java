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
package io.isima.bios.sdk.csdk;

import static com.fasterxml.uuid.Generators.timeBasedGenerator;

import io.isima.bios.codec.proto.messages.UuidMessageConverter;
import io.isima.bios.codec.proto.wrappers.ProtoInsertBulkResponse;
import io.isima.bios.codec.proto.wrappers.ProtoSelectRequestWriter;
import io.isima.bios.dto.AttributeSpec;
import io.isima.bios.dto.GetContextEntriesResponse;
import io.isima.bios.dto.IngestBulkErrorResponse;
import io.isima.bios.dto.MultiContextEntriesSpec;
import io.isima.bios.dto.MultiGetResponse;
import io.isima.bios.dto.MultiGetSpec;
import io.isima.bios.dto.PutContextEntriesRequest;
import io.isima.bios.dto.SelectContextEntriesResponse;
import io.isima.bios.dto.SelectContextRequest;
import io.isima.bios.dto.UpdateContextEntryRequest;
import io.isima.bios.models.AppType;
import io.isima.bios.models.AtomicOperationSpec;
import io.isima.bios.models.ContentRepresentation;
import io.isima.bios.models.Credentials;
import io.isima.bios.models.MultiSelectRequest;
import io.isima.bios.models.MultiSelectResponse;
import io.isima.bios.models.Record;
import io.isima.bios.models.SessionInfo;
import io.isima.bios.models.proto.DataProto;
import io.isima.bios.sdk.CompositeKey;
import io.isima.bios.sdk.ISql;
import io.isima.bios.sdk.csdk.codec.JsonPayloadCodec;
import io.isima.bios.sdk.csdk.codec.PayloadCodec;
import io.isima.bios.sdk.csdk.codec.ProtobufDecoders;
import io.isima.bios.sdk.csdk.codec.ProtobufPayloadEncoder;
import io.isima.bios.sdk.errors.BiosClientError;
import io.isima.bios.sdk.exceptions.BiosClientException;
import io.isima.bios.sdk.exceptions.IngestBulkBatchFailedException;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Setter;

public class BiosClient {
  // constants
  private static final long DEFAULT_OPERATION_TIMEOUT_MILLIS = 240000; // 4 minutes
  protected static final String TENANT_KEY = "tenant";
  protected static final String STREAM_KEY = "stream";
  protected static final String EVENT_ID_KEY = "eventId";
  protected static final String TENANT_NAME_KEY = "tenantName";
  protected static final String DETAIL_KEY = "detail";
  protected static final String INCLUDE_INTERNAL_KEY = "includeInternal";
  protected static final String KEY_ATOMIC_OP_ID = "atomicOpId";
  protected static final String KEY_ATOMIC_OP_COUNT = "atomicOpCount";

  // static resources
  private static final CSdkDirect csdk = new CSdkDirect();
  private static final PayloadCodec jsonCodec =
      new JsonPayloadCodec(BiosObjectMapperProvider.get());

  /** C-SDK session ID. */
  protected int sessionId;

  /** Tenant for the session. */
  @Setter private String tenantName;

  public BiosClient() {
    sessionId = -1;
  }

  /**
   * Check the payload size and termination to prevent crash on C-SDK side. Some libraries handles
   * the payload as a C string.
   *
   * @param payload The payload to check
   * @throws BiosClientException If the payload cannot be treated as a C string.
   */
  protected void verifyPayload(ByteBuffer payload) throws BiosClientException {
    int limit = payload.limit();
    if (limit >= payload.capacity()) {
      throw new BiosClientException("payload capacity must be larger than the data size");
    }
    payload.position(limit);
    payload.limit(payload.capacity());
    if (payload.get() != (byte) 0) {
      throw new BiosClientException("payload data must be terminated by 0");
    }
    // restore the original markers
    payload.clear();
    payload.limit(limit);
  }

  public long timestamp() {
    return csdk.currentTimeMillis();
  }

  protected CompletableFuture<Void> connectAsync(
      final String host, final int port, final String cafile) {
    CSdkApiCaller<StartSessionResponse> apiCaller =
        new CSdkApiCaller<>(
            -1,
            CSdkOperationId.START_SESSION,
            null,
            csdk,
            jsonCodec,
            StartSessionResponse.class,
            (opNumber, callback) -> {
              csdk.startSession(
                  host, port, true, "", cafile, DEFAULT_OPERATION_TIMEOUT_MILLIS, callback);
            });
    return apiCaller.invokeAsync().thenAccept((response) -> sessionId = response.getSessionId());
  }

  public void close() {
    synchronized (this) {
      if (sessionId >= 0) {
        csdk.endSession(sessionId);
        sessionId = -1;
      }
    }
  }

  public CSdkApiCaller<SessionInfo> makeLoginCaller(
      String email, String password, String appName, AppType appType) throws BiosClientException {
    final Credentials credentials = new Credentials(email, password, appName, appType);
    final ByteBuffer payload = jsonCodec.encode(credentials);
    verifyPayload(payload);
    return new CSdkApiCaller<>(
        sessionId,
        CSdkOperationId.LOGIN_BIOS,
        null,
        csdk,
        jsonCodec,
        SessionInfo.class,
        (opNumber, callback) -> {
          csdk.loginBios(sessionId, payload, payload.limit(), callback);
        });
  }

  public CSdkApiCaller<MultiSelectResponse> makeSelectCaller(
      MultiSelectRequest request, ByteBuffer timeStampBuffer) throws BiosClientException {
    final DataProto.SelectRequest req = ((ProtoSelectRequestWriter) request).toProto();
    final ByteBuffer payload = ProtobufPayloadEncoder.encode(req);
    return new CSdkApiCaller<>(
        sessionId,
        CSdkOperationId.SELECT_PROTO,
        request.queries().get(0).getFrom(),
        csdk,
        jsonCodec,
        ProtobufDecoders::decodeSelectResponse,
        MultiSelectResponse.class,
        (opNumber, callback) -> {
          csdk.simpleMethod(
              sessionId,
              opNumber,
              tenantName,
              request.queries().get(0).getFrom(),
              payload,
              payload.limit(),
              callback,
              timeStampBuffer);
        });
  }

  public CSdkApiCaller<Record> makeInsertCaller(
      ISql.Insert statement,
      AtomicOperationSpec atomicOperationSpec,
      Long signalVersion,
      ByteBuffer timeStampBuffer) {
    // make resources and options
    final var resources = fillInsertResources(statement.getSignalName());
    final var options = makeAtomicOptions(atomicOperationSpec);

    // make the payload
    final DataProto.InsertRequest req =
        DataProto.InsertRequest.newBuilder()
            .setContentRep(ContentRepresentation.CSV.toProto())
            .setSchemaVersion(signalVersion)
            .setRecord(
                DataProto.Record.newBuilder().addStringValues(statement.getCsvs().get(0)).build())
            .build();
    final ByteBuffer payload = ProtobufPayloadEncoder.encode(req);

    // create the caller
    return new CSdkApiCaller<>(
            sessionId,
            CSdkOperationId.INSERT_PROTO,
            statement.getSignalName(),
            csdk,
            jsonCodec,
            ProtobufDecoders::decodeInsertResponse,
            Record.class,
            (opNumber, callback) -> {
              csdk.genericMethod(
                  sessionId,
                  opNumber,
                  resources,
                  options,
                  payload,
                  payload.limit(),
                  callback,
                  timeStampBuffer);
            })
        .addTimestampBuffer(timeStampBuffer);
  }

  protected String[] fillInsertResources(String signalName) {
    return new String[] {
      TENANT_KEY, tenantName,
      STREAM_KEY, signalName,
      EVENT_ID_KEY, timeBasedGenerator().generate().toString()
    };
  }

  public CSdkApiCaller<Void> makeUpsertContextCaller(ISql.ContextUpsert statement)
      throws BiosClientException {
    PutContextEntriesRequest req = new PutContextEntriesRequest();
    req.setContentRepresentation(ContentRepresentation.CSV);
    req.setEntries(statement.getCsvs());

    final ByteBuffer payload = jsonCodec.encode(req);
    verifyPayload(payload);
    String[] resources =
        new String[] {TENANT_NAME_KEY, tenantName, STREAM_KEY, statement.getContextName()};
    final String[] options = new String[0];
    return new CSdkApiCaller<>(
            sessionId,
            CSdkOperationId.CREATE_CONTEXT_ENTRY_BIOS,
            statement.getContextName(),
            csdk,
            jsonCodec,
            Void.class,
            (opNumber, callback) ->
                csdk.genericMethod(
                    sessionId,
                    opNumber,
                    resources,
                    options,
                    payload,
                    payload.limit(),
                    callback,
                    null))
        .updateNumReadsWrites(0, req.getEntries().size());
  }

  public CSdkApiCaller<Void> makeContextUpdateCaller(ISql.ContextUpdate statement)
      throws BiosClientException {
    final var request = new UpdateContextEntryRequest();
    request.setContentRepresentation(ContentRepresentation.UNTYPED);
    request.setAttributes(
        statement.getAttributesToUpdate().entrySet().stream()
            .map((entry) -> new AttributeSpec(entry.getKey(), entry.getValue()))
            .collect(Collectors.toList()));
    final var primaryKeys = statement.getPrimaryKeyValues();
    if (primaryKeys == null || primaryKeys.isEmpty()) {
      throw new BiosClientException(
          BiosClientError.INVALID_REQUEST, "A primary key must be specified to request update");
    }
    if (primaryKeys.size() > 1) {
      throw new BiosClientException(
          BiosClientError.INVALID_REQUEST, "Multiple primary keys for update is not supported");
    }
    request.setPrimaryKey(primaryKeys.get(0).asObjectList());

    final ByteBuffer payload = jsonCodec.encode(request);
    verifyPayload(payload);
    return new CSdkApiCaller<>(
        sessionId,
        CSdkOperationId.UPDATE_CONTEXT_ENTRY_BIOS,
        statement.getContextName(),
        csdk,
        jsonCodec,
        Void.class,
        (opNumber, callback) -> {
          csdk.simpleMethod(
              sessionId,
              opNumber,
              tenantName,
              statement.getContextName(),
              payload,
              payload.limit(),
              callback,
              null);
        });
  }

  public CSdkApiCaller<GetContextEntriesResponse> makeGetContextEntriesCaller(
      ISql.ContextSelect statement) throws BiosClientException {
    MultiContextEntriesSpec spec = new MultiContextEntriesSpec();
    spec.setContentRepresentation(ContentRepresentation.UNTYPED);
    List<List<Object>> primaryKeyValuesDoubleList =
        (statement.getPrimaryKeyValues() == null)
            ? List.of()
            : convertCompositeKeys(statement.getPrimaryKeyValues());
    spec.setPrimaryKeys(primaryKeyValuesDoubleList);
    if (statement.getSelectColumns() != null && statement.getSelectColumns().size() > 0) {
      spec.setAttributes(statement.getSelectColumns());
    }
    if (statement.getOnTheFly() == Boolean.TRUE) {
      spec.setOnTheFly(true);
    }

    final ByteBuffer payload = jsonCodec.encode(spec);
    verifyPayload(payload);
    return new CSdkApiCaller<>(
        sessionId,
        CSdkOperationId.GET_CONTEXT_ENTRIES_BIOS,
        statement.getContextName(),
        csdk,
        jsonCodec,
        GetContextEntriesResponse.class,
        (opNumber, callback) -> {
          csdk.simpleMethod(
              sessionId,
              opNumber,
              tenantName,
              statement.getContextName(),
              payload,
              payload.limit(),
              callback,
              null);
        });
  }

  public CSdkApiCaller<SelectContextEntriesResponse> makeSelectContextEntriesCaller(
      SelectContextRequest request) throws BiosClientException {
    final ByteBuffer payload = jsonCodec.encode(request);
    verifyPayload(payload);

    return new CSdkApiCaller<>(
        sessionId,
        CSdkOperationId.SELECT_CONTEXT_ENTRIES,
        request.getContext(),
        csdk,
        jsonCodec,
        SelectContextEntriesResponse.class,
        (opNumber, callback) -> {
          csdk.simpleMethod(
              sessionId,
              opNumber,
              tenantName,
              request.getContext(),
              payload,
              payload.limit(),
              callback,
              null);
        });
  }

  public CSdkApiCaller<MultiGetResponse> makeMultiGetContextCaller(MultiGetSpec request)
      throws BiosClientException {
    final ByteBuffer payload = jsonCodec.encode(request);
    verifyPayload(payload);

    return new CSdkApiCaller<>(
        sessionId,
        CSdkOperationId.MULTI_GET_CONTEXT_ENTRIES_BIOS,
        null,
        csdk,
        jsonCodec,
        MultiGetResponse.class,
        (opNumber, callback) -> {
          csdk.simpleMethod(
              sessionId, opNumber, tenantName, null, payload, payload.limit(), callback, null);
        });
  }

  public CSdkApiCaller<Void> makeDeleteContextEntryCaller(ISql.ContextDelete statement)
      throws BiosClientException {
    MultiContextEntriesSpec spec = new MultiContextEntriesSpec();
    spec.setContentRepresentation(ContentRepresentation.UNTYPED);
    final var primaryKeys = statement.getPrimaryKeyValues();
    if (primaryKeys == null || primaryKeys.isEmpty()) {
      throw new BiosClientException(
          BiosClientError.INVALID_REQUEST, "Primary keys are required to request delete");
    }
    spec.setPrimaryKeys(convertCompositeKeys(primaryKeys));

    final ByteBuffer payload = jsonCodec.encode(spec);
    verifyPayload(payload);

    return new CSdkApiCaller<>(
        sessionId,
        CSdkOperationId.DELETE_CONTEXT_ENTRY_BIOS,
        statement.getContextName(),
        csdk,
        jsonCodec,
        Void.class,
        (opNumber, callback) -> {
          csdk.simpleMethod(
              sessionId,
              opNumber,
              tenantName,
              statement.getContextName(),
              payload,
              payload.limit(),
              callback,
              null);
        });
  }

  /**
   * Makes options for an atomic mutation.
   *
   * <p>If atomicity is not specified, the method returns an empty options.
   *
   * <p>Note that the method assumes there are no other options to add. The method cannot be used
   * for accumulating options.
   */
  public String[] makeAtomicOptions(AtomicOperationSpec atomicOperationSpec) {
    final String[] options;
    if (atomicOperationSpec != null) {
      options =
          new String[] {
            KEY_ATOMIC_OP_ID, atomicOperationSpec.getAtomicOperationId().toString(),
            KEY_ATOMIC_OP_COUNT, Integer.toString(atomicOperationSpec.getNumBundledRequests()),
          };
    } else {
      options = new String[0];
    }
    return options;
  }

  // methods used for insert bulk execution ///////

  /**
   * Kicks off a bulk insertion.
   *
   * @param numEvents Number of events to insert.
   * @param pagingCallback The object to receive the paging callbacks.
   * @return Insert bulk operation context ID
   */
  public long insertBulkStart(int numEvents, InsertBulkExecutor pagingCallback) {
    return csdk.ingestBulkStart(
        sessionId, CSdkOperationId.INSERT_BULK_PROTO.value(), numEvents, pagingCallback);
  }

  public CSdkApiCaller<ProtoInsertBulkResponse> makeInsertBulkCaller(
      String signalName,
      Long signalVersion,
      long insertBulkContextId,
      List<UUID> eventIds,
      List<String> eventTexts,
      int fromIndex,
      int toIndex,
      AtomicOperationSpec atomicOperationSpec) {

    final var options = makeAtomicOptions(atomicOperationSpec);

    final var requestPayload =
        makeInsertBulkPayload(
            signalName,
            signalVersion,
            eventIds.subList(fromIndex, toIndex),
            eventTexts.subList(fromIndex, toIndex));

    return new CSdkApiCaller<>(
            sessionId,
            CSdkOperationId.INSERT_BULK_PROTO,
            signalName,
            csdk,
            jsonCodec,
            ProtobufDecoders::decodeInsertBulkResponse,
            ProtoInsertBulkResponse.class,
            (opNumber, callback) -> {
              csdk.ingestBulk(
                  sessionId,
                  insertBulkContextId,
                  fromIndex,
                  toIndex,
                  options,
                  requestPayload,
                  requestPayload.limit(),
                  callback,
                  null);
            })
        .setErrorDecoder(
            (codec, errorPayload, status, endpoint) -> {
              try {
                final IngestBulkErrorResponse errorResponse =
                    codec.decode(errorPayload, IngestBulkErrorResponse.class);
                final var resultsWithError = errorResponse.getResultsWithError();
                if (resultsWithError == null || resultsWithError.isEmpty()) {
                  return new BiosClientException(status, errorResponse.getMessage(), endpoint);
                } else {
                  return new IngestBulkBatchFailedException(
                      status, errorResponse.getMessage(), resultsWithError);
                }
              } catch (BiosClientException e) {
                return e;
              }
            });
  }

  public void insertBulkEnd(long operationContext, int sessionId, long insertBulkContextId) {
    csdk.ingestBulkEnd(operationContext, sessionId, insertBulkContextId);
  }

  private ByteBuffer makeInsertBulkPayload(
      String signalName,
      Long signalVersion,
      List<UUID> subsetEventIds,
      List<String> subsetEventTexts) {
    final int size = subsetEventIds.size();
    DataProto.Record.Builder recordBuilder = DataProto.Record.newBuilder();
    final DataProto.InsertBulkRequest request =
        DataProto.InsertBulkRequest.newBuilder()
            .addAllRecord(
                IntStream.range(0, size)
                    .mapToObj(
                        (idx) -> {
                          DataProto.Record rec =
                              recordBuilder
                                  .setEventId(
                                      UuidMessageConverter.toProtoUuid(subsetEventIds.get(idx)))
                                  .addStringValues(subsetEventTexts.get(idx))
                                  .build();
                          recordBuilder.clear();
                          return rec;
                        })
                    .collect(Collectors.toList()))
            .setContentRep(DataProto.ContentRepresentation.CSV)
            .setSchemaVersion((signalVersion == null) ? 0 : signalVersion)
            .setSignal(signalName)
            .build();
    return ProtobufPayloadEncoder.encode(request);
  }

  ///////////////////////////////////////////////////////////////////////////////

  public <ReqT, RespT> CSdkApiCaller<RespT> makeGenericMethodCaller(
      CSdkOperationId opId,
      String streamName,
      String[] resources,
      String[] options,
      ReqT request,
      Class<RespT> responseClass)
      throws BiosClientException {
    final ByteBuffer payload;
    final long length;
    if (request != null) {
      payload = jsonCodec.encode(request);
      length = payload.limit();
    } else {
      payload = null;
      length = 0;
    }
    return new CSdkApiCaller<>(
        sessionId,
        opId,
        streamName,
        csdk,
        jsonCodec,
        responseClass,
        (opNumber, callback) -> {
          csdk.genericMethod(
              sessionId, opNumber, resources, options, payload, length, callback, null);
        });
  }

  public <ReqT, RespT> CSdkApiCaller<RespT> makeSimpleMethodCaller(
      CSdkOperationId opId, String streamName, ReqT request, Class<RespT> responseClass)
      throws BiosClientException {
    final ByteBuffer payload;
    final long length;
    if (request != null) {
      payload = jsonCodec.encode(request);
      length = payload.limit();
    } else {
      payload = null;
      length = 0;
    }
    return new CSdkApiCaller<>(
        sessionId,
        opId,
        streamName,
        csdk,
        jsonCodec,
        responseClass,
        (opNumber, callback) -> {
          csdk.simpleMethod(
              sessionId, opNumber, tenantName, streamName, payload, length, callback, null);
        });
  }

  protected List<List<Object>> convertCompositeKeys(List<CompositeKey> keys) {
    return keys.stream().map((key) -> key.asObjectList()).collect(Collectors.toList());
  }
}
