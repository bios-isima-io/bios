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

import io.isima.bios.exceptions.BiosServerException;
import io.isima.bios.exceptions.InvalidRequestSyntaxException;
import io.isima.bios.exceptions.MethodNotAllowedException;
import io.isima.bios.exceptions.PathNotFoundException;
import io.isima.bios.server.services.RequestStream;
import io.isima.bios.server.services.RestHandler;
import io.isima.bios.service.Keywords;
import io.netty.handler.codec.Headers;
import io.netty.handler.codec.http.HttpMethod;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HandlerNode {
  private static final Logger logger = LoggerFactory.getLogger(HandlerNode.class);

  private final String name;
  private final boolean isPathParameter;
  private final List<RestHandler<?, ?, ?>> handlers;
  private final Map<String, HandlerNode> children;
  private HandlerNode paramChild;
  private String allowedMethods;

  public HandlerNode(String name) {
    Objects.requireNonNull(name);
    this.name = name;
    this.isPathParameter = name.startsWith("{");
    if (isPathParameter && !name.endsWith("}")) {
      throw new IllegalArgumentException(
          "Syntax error: Name starts with open brace, but it's not closed");
    }
    this.handlers = new ArrayList<>();
    children = new HashMap<>();
    allowedMethods = "OPTIONS";
  }

  public HandlerNode(String name, RestHandler<?, ?, ?> handler) {
    this(name);
    setHandler(handler);
  }

  public RestHandler<?, ?, ?> getHandler(
      HttpMethod method, Headers<CharSequence, CharSequence, ?> requestHeaders)
      throws MethodNotAllowedException, PathNotFoundException {
    if (handlers.isEmpty()) {
      throw new PathNotFoundException();
    }
    if (method == HttpMethod.OPTIONS) {
      // HACK: put allowed methods in the request headers to inform the options handler what are
      // supported
      requestHeaders.add(Keywords.X_BIOS_ALLOWED_METHODS, allowedMethods);
      return BiosServer.getInstance().getMiscService().getOptionsHandler();
    }
    for (var handler : handlers) {
      if (handler.getMethod().equals(method)) {
        return handler;
      }
    }
    throw new MethodNotAllowedException(requestHeaders.get(":method").toString());
  }

  public void setHandler(RestHandler<?, ?, ?> handler) {
    Objects.requireNonNull("handler may not be null");
    if (this.handlers.stream().anyMatch((entry) -> entry.getMethod().equals(handler.getMethod()))) {
      throw new IllegalArgumentException(
          String.format("duplicate handler, new=%s, existing=%s", handler, handlers));
    }
    allowedMethods += ", " + handler.getMethod();
    this.handlers.add(handler);
  }

  public HandlerNode next(String element, RequestStream stream) throws BiosServerException {
    var nextNode = children != null ? children.get(element) : null;
    if (nextNode == null && paramChild != null) {
      try {
        stream.putPathParam(paramChild.name, URLDecoder.decode(element, "UTF-8"));
      } catch (UnsupportedEncodingException e) {
        throw new InvalidRequestSyntaxException(
            "Failed to URL decode path parameter %s=%s: %s",
            paramChild.name, element, e.getMessage());
      }
      nextNode = paramChild;
    }
    return nextNode;
  }

  public void addHandler(RestHandler<?, ?, ?> handler) {
    if (!handler.getPath().startsWith("/")) {
      throw new IllegalArgumentException("handler path must start with /");
    }
    addHandlerInternal(handler.getPath().substring(1), handler);
  }

  private void addHandlerInternal(String path, RestHandler<?, ?, ?> handler) {
    final var elements = path.split("/", 2);
    final var name = elements[0];
    final HandlerNode node;

    if (name.startsWith("{")) {
      if (!name.endsWith("}")) {
        throw new IllegalArgumentException(
            String.format("Invalid path parameter syntax: %s", name));
      }
      final var keyword = name.substring(1, name.length() - 1);
      if (paramChild == null) {
        paramChild = new HandlerNode(keyword);
      }
      if (!paramChild.name.equals(keyword)) {
        throw new IllegalArgumentException(
            String.format(
                "Path parameter must be consistent among paths; param=%s, existing=%s path=%s",
                keyword, paramChild.name, handler.getPath()));
      }
      node = paramChild;
      if (!children.isEmpty()) {
        final var fullPath = handler.getPath();
        final var conflict =
            String.format(
                "%s%s",
                fullPath.substring(0, fullPath.length() - path.length()),
                children.keySet().iterator().next());
        logger.warn(
            "A path parameter conflicts with a path element;\n"
                + "            path: {}\n  conflicts with: {}",
            fullPath,
            conflict);
      }
    } else {
      var child = children.get(name);
      if (child == null) {
        child = new HandlerNode(name);
        children.put(name, child);
      }
      node = child;
      if (paramChild != null) {
        final var fullPath = handler.getPath();
        final var conflict =
            String.format(
                "%s{%s}",
                fullPath.substring(0, fullPath.length() - path.length()), paramChild.name);
        logger.warn(
            "A path element conflicts with a path parameter;\n"
                + "            path: {}\n  conflicts with: {}",
            fullPath,
            conflict);
      }
    }

    if (elements.length == 1 || elements[1].isBlank()) {
      node.setHandler(handler);
    } else {
      node.addHandlerInternal(elements[1], handler);
    }
  }

  public void collectServices(List<String> services) {
    for (var handler : handlers) {
      services.add(
          String.format(
              "%6s %s  (%s)",
              handler.getMethod().toString(), handler.getPath(), handler.getOperationName()));
    }
    if (paramChild != null) {
      paramChild.collectServices(services);
    }
    final var sortedChildren = new TreeMap<>(children);
    sortedChildren.forEach((name, node) -> node.collectServices(services));
  }

  @Override
  public String toString() {
    final var sb =
        new StringBuilder("{name=")
            .append(name)
            .append(", handlers=")
            .append(handlers)
            .append(", children=")
            .append(children.keySet());
    if (paramChild != null) {
      sb.append(", paramChild=").append(paramChild.name);
    }
    return sb.append("}").toString();
  }
}
