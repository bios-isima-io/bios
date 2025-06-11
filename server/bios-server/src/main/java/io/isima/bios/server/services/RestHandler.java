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

import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.server.MediaType;
import io.isima.bios.service.Keywords;
import io.netty.handler.codec.Headers;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.DefaultCookie;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RestHandler<RequestT, ResponseT, StateT extends ExecutionState> {

  static final Logger logger = LoggerFactory.getLogger(RestHandler.class);

  protected final String operationName;
  protected final String path;
  protected final HttpMethod method;
  protected final MediaType requestMediaType;
  protected final MediaType responseMediaType;
  protected final Class<RequestT> requestClass;
  protected final Class<ResponseT> responseClass;
  protected final Class<StateT> stateClass;
  public final HttpResponseStatus successStatus;

  protected RestHandler(
      String operationName,
      HttpMethod method,
      String path,
      HttpResponseStatus successStatus,
      MediaType requestMediaType,
      MediaType responseMediaType,
      Class<RequestT> requestClass,
      Class<ResponseT> responseClass,
      Class<StateT> stateClass) {
    this.operationName = operationName;
    this.method = method;
    this.path = path;
    this.requestClass = requestClass;
    this.responseClass = responseClass;
    this.stateClass = stateClass;
    this.requestMediaType = requestMediaType;
    this.responseMediaType = responseMediaType;
    this.successStatus = successStatus;
  }

  public String getOperationName() {
    return operationName;
  }

  public String getPath() {
    return path;
  }

  public HttpMethod getMethod() {
    return method;
  }

  public MediaType getRequestMediaType() {
    return requestMediaType;
  }

  public MediaType getResponseMediaType() {
    return responseMediaType;
  }

  public Class<RequestT> getRequestClass() {
    return requestClass;
  }

  public Class<ResponseT> getResponseClass() {
    return responseClass;
  }

  public final CompletableFuture<ResponseT> handleRequest(
      Object request,
      Headers<CharSequence, CharSequence, ?> requestHeaders,
      Map<String, String> pathParams,
      Map<String, String> queryParams,
      Headers<CharSequence, CharSequence, ?> responseHeaders,
      ExecutionState state) {
    return handle(
            requestClass.cast(request),
            requestHeaders,
            pathParams,
            queryParams,
            responseHeaders,
            stateClass.cast(state))
        .thenApply(
            (response) -> {
              if (state.getUserContext() != null
                  && state.getUserContext().getNewSessionToken() != null) {
                final var cookie = makeCookie(state.getUserContext().getNewSessionToken());
                logger.debug(
                    "for user={} scope={} appName={}, adding set-cookie: {}",
                    state.getUserContext().getSubject(),
                    state.getUserContext().getScope(),
                    state.getUserContext().getAppName(),
                    cookie);
                responseHeaders.add(
                    HttpHeaderNames.SET_COOKIE, ServerCookieEncoder.STRICT.encode(cookie));
              }
              return response;
            });
  }

  public abstract StateT createState(Executor executor);

  protected abstract CompletableFuture<ResponseT> handle(
      RequestT request,
      Headers<CharSequence, CharSequence, ?> requestHeaders,
      Map<String, String> pathParams,
      Map<String, String> queryParams,
      Headers<CharSequence, CharSequence, ?> responseHeaders,
      StateT state);

  protected final GenericExecutionState createDefaultState(Executor executor) {
    return new GenericExecutionState(operationName, executor);
  }

  public static Cookie makeCookie(String token) {
    final var cookie = new DefaultCookie(Keywords.TOKEN, token);
    cookie.setDomain("");
    cookie.setPath("/");
    cookie.setHttpOnly(true);
    cookie.setSecure(true);
    return cookie;
  }

  @Override
  public String toString() {
    return new StringBuilder("{opName=")
        .append(operationName)
        .append(", method=")
        .append(method)
        .append(", path=")
        .append(path)
        .append("}")
        .toString();
  }
}
