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
package io.isima.bios.server.services;

import io.isima.bios.configuration.Bios2Config;
import io.isima.bios.errors.AuthError;
import io.isima.bios.errors.exception.AuthenticationException;
import io.isima.bios.errors.exception.DataValidationError;
import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.BiosServerException;
import io.isima.bios.exceptions.InsertBulkFailedServerException;
import io.isima.bios.exceptions.PathNotFoundException;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.metrics.OperationMetricsTracer;
import io.isima.bios.models.ErrorResponsePayload;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.server.BiosServer;
import io.isima.bios.server.MediaType;
import io.isima.bios.service.Keywords;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.CompositeByteBuf;
import io.netty.handler.codec.Headers;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.concurrent.EventExecutor;
import java.io.InputStream;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import javax.ws.rs.core.Response;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that holds parameters and contexts of a request.
 *
 * <p>The class is instantiated when the server receives a new HTTP/2 stream. Terminates when the
 * the request is handled.
 */
public abstract class RequestStream<HeadersT extends Headers<CharSequence, CharSequence, ?>, IdT> {
  private static final Logger logger = LoggerFactory.getLogger(RequestStream.class);

  // The main executor for the stream
  protected final EventExecutor eventExecutor;

  @Getter private final SocketAddress peerAddress;

  // HTTP request parameters
  @Getter private final HeadersT requestHeaders;
  private HttpMethod method;
  private CharSequence authority;
  private String path;
  private MediaType contentType;
  private int contentLength;
  private Map<String, String> pathParams;
  private Map<String, String> queryParams;

  // HTTP response paramters
  @Getter protected final HeadersT responseHeaders;

  // HTTP request payload, not decoded
  private ByteBuf requestPayload;

  // REST handler
  private RestHandler<?, ?, ?> handler;

  // Root execution state for this strem. Assigned when the instance starts handling the op.
  @Getter ExecutionState rootState;

  // metrics
  @Getter protected final OperationMetricsTracer metricsTracer;

  public RequestStream(
      EventExecutor eventExecutor,
      HeadersT requestHeaders,
      HeadersT responseHeaders,
      SocketAddress peerAddress) {
    metricsTracer = new OperationMetricsTracer(0);
    this.eventExecutor = eventExecutor;
    this.requestHeaders = requestHeaders;
    this.responseHeaders = responseHeaders;
    this.responseHeaders.add(HttpHeaderNames.SERVER, BiosServer.SERVER_NAME);
    this.peerAddress = peerAddress;
    final var methodSrc = requestHeaders.get(Keywords.METHOD);
    method = HttpMethod.valueOf(methodSrc != null ? methodSrc.toString() : "");
    final var temp = requestHeaders.get(Keywords.PATH);
    path = temp != null ? temp.toString() : "";

    authority = requestHeaders.get(Keywords.AUTHORITY);
    if (requestHeaders.contains(HttpHeaderNames.CONTENT_TYPE)) {
      final var contentTypeSrc = requestHeaders.get(HttpHeaderNames.CONTENT_TYPE).toString();
      final var index = contentTypeSrc.indexOf(";");
      if (index >= 0) {
        contentType = MediaType.resolve(contentTypeSrc.substring(0, index));
        // parse params and put the values as header fields ".content-type.{paramName}"
        final var paramEntries = contentTypeSrc.substring(index + 1).split(",");
        for (var entry : paramEntries) {
          final var keyValue = entry.trim().split("=", 2);
          if (keyValue.length == 2) {
            requestHeaders.add(".content-type." + keyValue[0], keyValue[1]);
          }
        }
      } else {
        contentType = MediaType.resolve(contentTypeSrc);
      }
    } else {
      // TODO throw
    }
    if (requestHeaders.contains(HttpHeaderNames.CONTENT_LENGTH)) {
      contentLength =
          Integer.parseInt(requestHeaders.get(HttpHeaderNames.CONTENT_LENGTH).toString());
    } else {
      contentLength = 0;
    }
    metricsTracer.setBytesWritten(contentLength);
    requestPayload = null;

    final var origin = requestHeaders.get(HttpHeaderNames.ORIGIN);
    if (origin != null && Bios2Config.getCorsOriginWhiteList().isAllowed(origin)) {
      responseHeaders.add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    }
    responseHeaders.add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
  }

  public abstract IdT getId();

  protected abstract <T> void sendResponse(T content, MediaType contentType)
      throws BiosServerException;

  public EventExecutor getEventExecutor() {
    return eventExecutor;
  }

  public HttpMethod getMethod() {
    return method;
  }

  public CharSequence getPath() {
    return path;
  }

  public Headers<CharSequence, CharSequence, ?> getRequestHeaders() {
    return requestHeaders;
  }

  public ByteBuf getContent() {
    return requestPayload;
  }

  public void releaseContent() {
    if (requestPayload != null) {
      requestPayload.release();
      requestPayload = null;
    }
  }

  public InputStream getContentInputStream() {
    return requestPayload != null ? new ByteBufInputStream(requestPayload) : null;
  }

  public void addContent(ByteBuf content) {
    Objects.requireNonNull(content, "incoming content buffer must not be null");
    if (this.requestPayload == null) {
      this.requestPayload = content;
      return;
    }
    final CompositeByteBuf compositeBuffer;
    if ((this.requestPayload instanceof CompositeByteBuf)) {
      compositeBuffer = (CompositeByteBuf) this.requestPayload;
    } else {
      compositeBuffer = ByteBufAllocator.DEFAULT.compositeBuffer();
      compositeBuffer.addComponent(true, this.requestPayload);
      this.requestPayload = compositeBuffer;
    }
    compositeBuffer.addComponent(true, content);
  }

  public RestHandler<?, ?, ?> getHandler() {
    return handler;
  }

  public void setHandler(RestHandler<?, ?, ?> handler) {
    Objects.requireNonNull(handler);
    this.handler = handler;
  }

  public Map<String, String> getPathParams() {
    return pathParams;
  }

  public void putPathParam(String name, String value) {
    if (pathParams == null) {
      pathParams = new HashMap<>();
    }
    pathParams.put(name, value);
  }

  public Map<String, String> getQueryParams() {
    return queryParams;
  }

  public void putQueryParam(String name, String value) {
    if (queryParams == null) {
      queryParams = new HashMap<>();
    }
    queryParams.put(name, value);
  }

  public void start() {
    rootState = handler.createState(eventExecutor);
    rootState.setMetricsTracer(metricsTracer);
  }

  public Object supplyContent() throws CompletionException {
    rootState.addHistory("(decodeRequest");
    final Object requestBody;
    if (handler.requestClass == Void.class) {
      requestBody = null;
    } else {
      try {
        if (requestPayload == null) {
          throw new InvalidRequestException("Request body is required but not given");
        }
        requestBody =
            handler.requestMediaType.payloadCodec().decode(requestPayload, handler.requestClass);
        final var metricsTracer = rootState.getMetricsTracer();
        if (metricsTracer != null) {
          metricsTracer.doneDecoding();
          metricsTracer.startValidation();
        }
      } catch (TfosException e) {
        throw new CompletionException(e);
      }
    }
    logger.trace("request={} ({})", requestBody, handler.requestClass);
    return requestBody;
  }

  public CompletableFuture<?> handleRequest(Object request) {
    rootState.addHistory(")(handle");
    return handler.handleRequest(
        request, requestHeaders, pathParams, queryParams, responseHeaders, rootState);
  }

  public <T> void reply(T result) throws BiosServerException {
    rootState.addHistory(")(reply");
    responseHeaders.set(Keywords.STATUS, handler.successStatus.codeAsText());
    metricsTracer.endPostProcess();
    metricsTracer.startEncoding(true);
    sendResponse(result, handler.getResponseMediaType());

    dumpHeader();
    rootState.addHistory(")");
  }

  public void handleError(Throwable cause) {
    final Object data;
    final MediaType mediaType;
    final var errorContext = makeErrorContext();
    final var errorLogContext = makeErrorLogContext(errorContext);
    final var userContext = rootState != null ? rootState.getUserContext() : null;
    if (cause instanceof TfosException) {
      final var te = (TfosException) cause;
      if (rootState != null && !(cause instanceof InsertBulkFailedServerException)) {
        rootState.addError(cause instanceof DataValidationError);
      }
      te.setUserContext(userContext);
      String errorMessage = "";
      if (te.getStatus() != Response.Status.INTERNAL_SERVER_ERROR) {
        if ((te instanceof AuthenticationException)
            && !Set.of(
                    AuthError.SESSION_EXPIRED.getErrorCode(),
                    AuthError.USER_ID_NOT_VERIFIED.getErrorCode())
                .contains(te.getErrorCode())) {
          errorMessage = "Authentication failed";
        } else {
          errorMessage = te.getMessage() != null ? te.getMessage() : "";
          if (!errorMessage.contains("stream=")) {
            errorMessage = errorMessage + errorContext;
          }
        }
        logger.warn(
            "Operation failed; op={}, error={}: {}{}",
            handler.getOperationName(),
            cause.getClass().getSimpleName(),
            cause.getMessage(),
            errorLogContext);
      } else {
        logger.error(
            "Operation failed; op={}{}", handler.getOperationName(), errorLogContext, cause);
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Call trace;\n{}", getCallTrace(rootState));
      }
      final String status = Integer.toString(te.getStatus().getStatusCode());
      responseHeaders.set(Keywords.STATUS, status);
      if (te instanceof InsertBulkFailedServerException) {
        data = ((InsertBulkFailedServerException) te).getErrorResponse();
      } else {
        data = new ErrorResponsePayload(te.getErrorCode(), te.getStatus(), errorMessage);
      }
      mediaType = MediaType.APPLICATION_JSON;
    } else {
      if (handler != null) {
        logger.error(
            "Operation error; op={}{}: {}",
            handler.getOperationName(),
            errorLogContext,
            cause.toString(),
            cause);
        logger.error("Call trace;\n{}", getCallTrace(rootState));
      }
      if (cause instanceof BiosServerException) {
        final var ex = (BiosServerException) cause;
        final var status = ex.getStatus();
        final var sb =
            new StringBuilder(status.codeAsText()).append(" ").append(status.reasonPhrase());
        if (ex.getMessage() != null) {
          sb.append(": ").append(ex.getMessage());
        }
        responseHeaders.set(Keywords.STATUS, status.codeAsText());
        data = sb.toString();
        mediaType = MediaType.TEXT_PLAIN;
      } else {
        final var status =
            cause instanceof PathNotFoundException
                ? HttpResponseStatus.NOT_FOUND
                : HttpResponseStatus.INTERNAL_SERVER_ERROR;
        responseHeaders.set(Keywords.STATUS, status.codeAsText());
        final var sb =
            new StringBuilder(status.codeAsText()).append(" ").append(status.reasonPhrase());
        data = sb.toString();
        mediaType = MediaType.TEXT_PLAIN;
      }
    }
    try {
      metricsTracer.startEncoding(false);
      sendResponse(data, mediaType);
      // replyInternal(data, mediaType);
      dumpHeader();
    } catch (BiosServerException e) {
      logger.error("Error during replying the client, giving up the operation", e);
    }
  }

  private String makeErrorContext() {
    if (rootState == null) {
      return "";
    }
    final var sb = new StringBuilder();
    String delimiter = "; ";
    final var streamDesc = rootState.getStreamDesc();
    if (streamDesc != null) {
      final var streamType = streamDesc.getType() == StreamType.CONTEXT ? "context=" : "signal=";
      sb.append(delimiter).append("tenant=").append(streamDesc.getParent().getName());
      delimiter = ", ";
      sb.append(delimiter).append(streamType).append(streamDesc.getName());
    } else if (rootState.getTenantName() != null) {
      sb.append(delimiter).append("tenant=").append(rootState.getTenantName());
      delimiter = ", ";
    }
    return sb.toString();
  }

  private String makeErrorLogContext(String errorContext) {
    final var userContext = rootState != null ? rootState.getUserContext() : null;
    final var errorLogContext = new StringBuilder();
    if (userContext != null) {
      errorLogContext.append(
          errorContext.isEmpty()
              ? String.format("; user=%s", userContext.getSubject())
              : String.format("%s, user=%s", errorContext, userContext.getSubject()));
      errorLogContext
          .append(", appName=")
          .append(userContext.getAppName() != null ? userContext.getAppName() : "");
      errorLogContext
          .append(", appType=")
          .append(userContext.getAppType() != null ? userContext.getAppType() : "");
    } else {
      errorLogContext.append(errorContext);
    }
    if (rootState != null) {
      String delimiter = errorContext.isEmpty() ? "" : ", ";
      for (var entry : rootState.getLogContext().entrySet()) {
        errorLogContext
            .append(delimiter)
            .append(entry.getKey())
            .append("=")
            .append(entry.getValue());
      }
      if (rootState.getExecutionId() != null) {
        errorLogContext.append(delimiter).append("execId=").append(rootState.getExecutionId());
      }
    }
    return errorLogContext.toString();
  }

  private Object getCallTrace(ExecutionState state) {
    return (state != null) ? state.getCallTraceString() : "unavailable";
  }

  private void dumpHeader() {
    if (Bios2Config.isHeaderDumpEnabled()) {
      final var sb =
          new StringBuilder()
              .append("\n----------------------------REQUEST---------------------------\n");
      getRequestHeaders()
          .forEach(
              (entry) -> sb.append(String.format("  %s: %s\n", entry.getKey(), entry.getValue())));
      sb.append("--------------------------RESPONSE--------------------------\n");
      responseHeaders.forEach(
          (entry) -> sb.append(String.format("  %s: %s\n", entry.getKey(), entry.getValue())));
      sb.append("\n==============================================================");
      logger.info("{}", sb);
    }
  }
}
