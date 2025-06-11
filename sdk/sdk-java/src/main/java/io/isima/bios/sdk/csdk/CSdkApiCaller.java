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

import com.fasterxml.jackson.core.type.TypeReference;
import io.isima.bios.codec.proto.wrappers.ProtoInsertBulkResponse;
import io.isima.bios.dto.GetContextEntriesResponse;
import io.isima.bios.models.DataWindow;
import io.isima.bios.models.ErrorResponsePayload;
import io.isima.bios.models.MultiSelectResponse;
import io.isima.bios.models.SelectResponse;
import io.isima.bios.sdk.csdk.codec.PayloadCodec;
import io.isima.bios.sdk.errors.BiosClientError;
import io.isima.bios.sdk.exceptions.BiosClientException;
import io.isima.bios.sdk.impl.BiosClientUtils;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to help executing C-SDK native API methods.
 *
 * <p>This class creates a CompletableFuture&lt;T&gt; on construction and holds it internally. The
 * execution status is updated there. The constructor also takes a dynamic function that sends a
 * request to a C-SDK native API method.
 *
 * <p>Execution starts when the user calls method {@link #invoke} or {@link #invokeAsync} which
 * invokes a native API method via the given {@link NativeMethodInvoker} object. The native API
 * would execute the call back method {@link #onCompletion} via {@link CallbackCenter#onCompletion}
 * that is registered to the C-API driver {@link CSdkDirect}. The method notifies the future the
 * completion, so that the user can handle completion via the CompletableFuture.
 *
 * <p>Following is an example usage of this class:
 *
 * <pre>
 * // Create and encode the request object
 * final Credentials credentials = new Credentials(user, password, scope);
 * final ByteBuffer payload = payloadCodec.ecode(credentials);
 *
 * // Create an instance of this class
 * AsyncNativeCall&lt;LoginResponse&gt; apiCaller =
 *     new AsyncNativeCall&lt;LoginReponse&gt;(payloadCodec, LoginResponse.class,
 *         (callback) -> {
 *           // The defined invoker must give the callback object to the native method.
 *           capi.login(payload, payload.limit(), callback);
 *         });
 *
 * // Start execution
 * final CompletableFuture<LoginResponse> future = call.invokeAsync();
 *
 * // Handle the response
 * future.thenApply((response) -> handleSuccessfulLogin(response));
 * </pre>
 *
 * @param <T> Type of the return object.
 */
public final class CSdkApiCaller<T> {
  private static final Logger logger = LoggerFactory.getLogger(CSdkApiCaller.class);

  private final CompletableFuture<T> future;
  private final CSdkDirect csdk;
  private final PayloadCodec payloadCodec;
  private final Class<T> clazz;
  private final TypeReference<T> typeReference;
  private final NativeMethodInvoker<T> nativeMethodInvoker;
  private final CSdkOperationId opId;
  private final DecoderFunction<T> decoderFunction;
  private final int sessionId;
  private final String stream;

  private ErrorDecoder errorDecoder;
  private ByteBuffer timestamps;
  private long startTimeUs;
  private long numReads;
  private long numWrites;
  private PostProcessor<T> postProcessor;

  /**
   * Dynamic function to invoke a C-SDK native method.
   *
   * @param <T> Type of the return object.
   */
  public interface NativeMethodInvoker<T> {
    /**
     * Invokes a C-API native call.
     *
     * <p>The defined method must give the callback object to the native API method. See the example
     * in the class description: {@link CSdkApiCaller}.
     *
     * @param opNumber Operation ID as integer
     * @param callback CSdkApiCaller object to be used for completion handling.
     */
    void invoke(int opNumber, CSdkApiCaller<T> callback);
  }

  public interface PostProcessor<T> {
    T postProcess(T orig, long timestampPosition);
  }

  /**
   * Decoder function to decode data.
   *
   * <p>If decoder function is specified, then it takes precedence over payload codec.
   *
   * @param <T> The API response object
   */
  @FunctionalInterface
  public interface DecoderFunction<T> {
    T decode(ByteBuffer payload) throws BiosClientException;
  }

  /** Generates BiosClientException by decoding an error payload sent by the server/c-sdk. */
  @FunctionalInterface
  public interface ErrorDecoder {
    BiosClientException decodeError(
        PayloadCodec codec, ByteBuffer payload, BiosClientError status, String endpoint);
  }

  /**
   * The constructor of the class.
   *
   * @param payloadCodec The payload codec to be used by completion handler {@link #onCompletion}
   * @param clazz Class object of the return type used for decoding the payload.
   * @param nativeMethodInvoker An Invoker object to be used for initiating the call.
   */
  public CSdkApiCaller(
      int sessionId,
      CSdkOperationId opId,
      String stream,
      CSdkDirect csdk,
      PayloadCodec payloadCodec,
      Class<T> clazz,
      NativeMethodInvoker<T> nativeMethodInvoker) {
    this(sessionId, opId, stream, csdk, payloadCodec, null, clazz, null, nativeMethodInvoker);
  }

  /**
   * Constructor that takes a decoder function.
   *
   * @param decoderFunction function used to decode a byte buffer
   */
  public CSdkApiCaller(
      int sessionId,
      CSdkOperationId opId,
      String stream,
      CSdkDirect csdk,
      PayloadCodec payloadCodec,
      DecoderFunction<T> decoderFunction,
      Class<T> clazz,
      NativeMethodInvoker<T> nativeMethodInvoker) {
    this(
        sessionId,
        opId,
        stream,
        csdk,
        payloadCodec,
        decoderFunction,
        clazz,
        null,
        nativeMethodInvoker);
  }

  /**
   * Constructs the native call with all parameters.
   *
   * @param opId Operation Id
   * @param csdk Reference to csdk endpoint
   * @param payloadCodec Codec to use, if decoderFunction is not specified
   * @param decoderFunction Decoder function to use to tranform a response bytebuffer
   * @param clazz Type of response
   * @param typeReference Type of response as type reference to extract type info
   * @param nativeMethodInvoker Csdk method to invoke
   */
  private CSdkApiCaller(
      int sessionId,
      CSdkOperationId opId,
      String stream,
      CSdkDirect csdk,
      PayloadCodec payloadCodec,
      DecoderFunction<T> decoderFunction,
      Class<T> clazz,
      TypeReference<T> typeReference,
      NativeMethodInvoker<T> nativeMethodInvoker) {
    future = new CompletableFuture<>();
    this.sessionId = sessionId;
    this.opId = opId;
    this.stream = stream;
    this.csdk = csdk;
    this.payloadCodec = payloadCodec;
    this.clazz = clazz;
    this.typeReference = typeReference;
    this.nativeMethodInvoker = nativeMethodInvoker;
    this.decoderFunction = decoderFunction;
    this.errorDecoder = CSdkApiCaller::decodeError;
    updateNumRecords();
  }

  /** Overrides the default error decoder by the specified one. */
  public CSdkApiCaller<T> setErrorDecoder(ErrorDecoder errorDecoder) {
    this.errorDecoder = errorDecoder;
    return this;
  }

  public CSdkApiCaller<T> addTimestampBuffer(ByteBuffer timestamps) {
    this.timestamps = timestamps;
    return this;
  }

  public CSdkApiCaller<T> addPostProcessor(PostProcessor<T> postProcessor) {
    this.postProcessor = postProcessor;
    return this;
  }

  public CSdkApiCaller<T> updateNumReadsWrites(int numReads, int numWrites) {
    this.numReads = numReads;
    this.numWrites = numWrites;
    return this;
  }

  /**
   * Method to initiate the call via the invoker.
   *
   * <p>The internal future is ready to listen after this call.
   *
   * @return Self.
   */
  public T invoke() throws BiosClientException {
    invokeAsync();
    return get();
  }

  public CompletableFuture<T> invokeAsync() {
    this.startTimeUs = System.nanoTime() / 1000;
    logger.debug("invoking operation " + opId.name());
    try {
      nativeMethodInvoker.invoke(opId.value(), this);
    } catch (Throwable t) {
      future.completeExceptionally(t);
    }
    return this.future;
  }

  /**
   * The callback method executed by the native API.
   *
   * @param errorNumber Error number returned by the API.
   * @param endpoint Server endpoint.
   * @param payload Response payload returned by the API.
   */
  public void onCompletion(
      int errorNumber,
      String endpoint,
      ByteBuffer payload,
      long timestampPosition,
      long csdkLatency,
      long qosRetryConsidered,
      long qosRetrySent,
      long qosRetryResponseUsed) {
    if (timestamps != null) {
      long timestamp = csdk.currentTimeMillis();
      timestamps.position((int) timestampPosition);
      // COMPLETION_DISPATCH
      timestamps.putLong(timestamp);
    }
    try {
      logger.debug(
          "onCompletion for operation "
              + opId.name()
              + ", status="
              + BiosClientUtils.statusToError(errorNumber));
      BiosClientError status = BiosClientUtils.statusToError(errorNumber);
      if (status == BiosClientError.OK) {
        T response;
        if (decoderFunction != null) {
          response = decoderFunction.decode(payload);
        } else {
          if (clazz != null) {
            response = payload != null ? (T) payloadCodec.decode(payload, clazz) : null;
          } else {
            response = payload != null ? (T) payloadCodec.decode(payload, typeReference) : null;
          }
        }

        if (clazz == GetContextEntriesResponse.class) {
          numReads = ((GetContextEntriesResponse) response).getEntries().size();
        } else if (clazz == MultiSelectResponse.class) {
          long reads = 0;
          for (SelectResponse resp : ((MultiSelectResponse) response).getSelectResponses()) {
            for (DataWindow window : resp.getDataWindows()) {
              reads += window.getRecords().size();
            }
          }
          numReads = reads;
        } else if (clazz == ProtoInsertBulkResponse.class) {
          numWrites = ((ProtoInsertBulkResponse) response).getResults().size();
        }

        logClientMetrics(
            errorNumber,
            stream,
            endpoint,
            csdkLatency,
            qosRetryConsidered,
            qosRetrySent,
            qosRetryResponseUsed);

        try {
          if (response != null && postProcessor != null) {
            response = postProcessor.postProcess(response, timestampPosition);
          }
          future.complete(response);
        } catch (Exception e) {
          future.completeExceptionally(
              new BiosClientException(
                  BiosClientError.CLIENT_CHANNEL_ERROR, e.getMessage(), endpoint));
        }
      } else {
        if (payload != null) {
          future.completeExceptionally(
              errorDecoder.decodeError(payloadCodec, payload, status, endpoint));
        } else {
          future.completeExceptionally(new BiosClientException(status, "", endpoint));
        }
      }
    } catch (BiosClientException e) {
      future.completeExceptionally(e);
    } finally {
      logger.debug("releasing payload");
      csdk.releasePayload(payload);
    }
  }

  /**
   * The getter to the internal future.
   *
   * @return
   */
  public CompletableFuture<T> getFuture() {
    return future;
  }

  /**
   * Calls future.get() internally.
   *
   * <p>The behavior is equivalent to getFuture().get() except an exception is translated to {@link
   * BiosClientException}. The method is useful to run a synchrnous call.
   *
   * @return The reply from the API.
   * @throws BiosClientException When the operation fails.
   */
  public T get() throws BiosClientException {
    try {
      return future.get();
    } catch (InterruptedException e) {
      logger.warn("Operation interrupted", e);
      Thread.currentThread().interrupt();
      return null;
    } catch (ExecutionException e) {
      if (e.getCause() instanceof BiosClientException) {
        BiosClientException orig = (BiosClientException) e.getCause();
        throw new BiosClientException(orig.getCode(), orig.getMessage(), orig.getEndpoint());
      } else {
        throw new BiosClientException(e);
      }
    }
  }

  /** Default error decoder. */
  private static BiosClientException decodeError(
      PayloadCodec codec, ByteBuffer payload, BiosClientError status, String endpoint) {
    try {
      final ErrorResponsePayload errorResponse = codec.decode(payload, ErrorResponsePayload.class);
      return new BiosClientException(status, errorResponse.getMessage(), endpoint);
    } catch (BiosClientException e) {
      return e;
    }
  }

  private void updateNumRecords() {
    switch (opId) {
      case INSERT_PROTO:
      case CREATE_CONTEXT_ENTRY_BIOS:
      case UPDATE_CONTEXT_ENTRY_BIOS:
      case DELETE_CONTEXT_ENTRY_BIOS:
      case REPLACE_CONTEXT_ENTRY_BIOS:
        numReads = 0;
        numWrites = 1;
        break;
      default:
        numReads = 0;
        numWrites = 0;
        break;
    }
  }

  private void logClientMetrics(
      int statusCode,
      String stream,
      String endpoint,
      long latencyInternalUs,
      long qosRetryConsidered,
      long qosRetrySent,
      long qosRetryResponseUsed) {
    if (sessionId == -1) {
      return;
    }

    if (stream == null) {
      stream = "";
    }

    String request = opId.getOpName();
    if (request == null) {
      request = "";
    }

    String status = csdk.getStatusName(statusCode);
    if (status == null) {
      status = "";
    }

    if (endpoint == null) {
      endpoint = "";
    }

    long latencyUs = System.nanoTime() / 1000 - startTimeUs;
    if (latencyInternalUs == 0) {
      latencyInternalUs = latencyUs;
    }

    csdk.log(
        sessionId,
        stream,
        request,
        status,
        endpoint,
        latencyUs,
        latencyInternalUs,
        numReads,
        numWrites,
        qosRetryConsidered,
        qosRetrySent,
        qosRetryResponseUsed);
  }
}
