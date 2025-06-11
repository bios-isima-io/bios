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
package io.isima.bios.server;

import static io.netty.handler.codec.http.HttpMethod.GET;

import io.isima.bios.exceptions.BiosServerException;
import io.isima.bios.exceptions.InvalidRequestSyntaxException;
import io.isima.bios.exceptions.PathNotFoundException;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.server.services.RequestStream;
import io.isima.bios.server.services.RestHandler;
import io.netty.handler.codec.Headers;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class ServiceRouter {
  private static final int STATE_PATH = 0;
  private static final int STATE_QUERY_PARAMS_KEY = 1;
  private static final int STATE_QUERY_PARAMS_VALUE = 2;
  private static final char EOF = 0;

  private final HandlerNode rootNode;

  public ServiceRouter() {
    final var rootHandler =
        new RestHandler<>(
            "Hello",
            GET,
            "/",
            HttpResponseStatus.OK,
            MediaType.NONE,
            MediaType.APPLICATION_JSON,
            Void.class,
            String.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return new GenericExecutionState(operationName, executor);
          }

          @Override
          protected CompletableFuture<String> handle(
              Void request,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            return CompletableFuture.completedFuture("Hello world");
          }
        };
    rootNode = new HandlerNode("", rootHandler);
  }

  public RestHandler<?, ?, ?> resolveHandler(RequestStream stream) throws BiosServerException {
    final var httpMethod = stream.getMethod();
    final var path = stream.getPath();
    // we assume the path always starts with '/'
    int start = 1;
    HandlerNode currentNode = rootNode;
    int state = STATE_PATH;
    final String delimiters = "/?=&";
    String key = null;
    for (int ich = 1; ich <= path.length(); ++ich) {
      final char ch = (ich < path.length()) ? path.charAt(ich) : EOF;
      if (ch == EOF || delimiters.indexOf(ch) >= 0) {
        final String token = path.subSequence(start, ich).toString();
        if (token.isEmpty() && ch == EOF) {
          break;
        }
        start = ich + 1;
        switch (state) {
          case STATE_PATH:
            currentNode = currentNode.next(token, stream);
            if (currentNode == null) {
              throw new PathNotFoundException(path.toString());
            }
            break;
          case STATE_QUERY_PARAMS_KEY:
            try {
              key = URLDecoder.decode(token, "UTF-8");
            } catch (UnsupportedEncodingException e) {
              throw new InvalidRequestSyntaxException(
                  "Failed to URL decode query parameter key %s: %s", token, e.getMessage());
            }
            break;
          case STATE_QUERY_PARAMS_VALUE:
            try {
              stream.putQueryParam(key, URLDecoder.decode(token, "UTF-8"));
            } catch (UnsupportedEncodingException e) {
              throw new InvalidRequestSyntaxException(
                  "Failed to URL decode query parameter value %s=%s: %s",
                  key, token, e.getMessage());
            }
            break;
          default:
            // do nothing
        }
        switch (ch) {
          case '?':
          case '&':
            state = STATE_QUERY_PARAMS_KEY;
            break;
          case '=':
            state = STATE_QUERY_PARAMS_VALUE;
            break;
          default:
            // don't change state
        }
      }
    }
    final var handler =
        currentNode != null ? currentNode.getHandler(httpMethod, stream.getRequestHeaders()) : null;
    if (handler == null) {
      throw new PathNotFoundException(path.toString());
    }
    stream.setHandler(handler);
    return handler;
  }

  public void addHandler(RestHandler<?, ?, ?> handler) {
    rootNode.addHandler(handler);
  }

  public List<String> collectServices() {
    final var result = new ArrayList<String>();
    rootNode.collectServices(result);
    return result;
  }
}
