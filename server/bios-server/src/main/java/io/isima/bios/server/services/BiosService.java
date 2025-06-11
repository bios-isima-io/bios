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

import static io.netty.handler.codec.http.HttpHeaderNames.AUTHORIZATION;
import static io.netty.handler.codec.http.HttpHeaderNames.COOKIE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.SessionToken;
import io.isima.bios.server.ServiceRouter;
import io.isima.bios.service.HttpClientManager;
import io.isima.bios.service.HttpFanRouter;
import io.isima.bios.service.Keywords;
import io.isima.bios.service.handler.FanRouter;
import io.isima.bios.utils.BiosObjectMapperProvider;
import io.netty.handler.codec.Headers;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BiosService {
  public static final Logger logger = LoggerFactory.getLogger(BiosService.class);

  // TODO(Naoki): Can we manage keywords better? Lowering case is necessary, but we don't want to
  // calculate lower case for each request
  private static final String AUTH_PREFIX = Keywords.BEARER.toLowerCase() + " ";

  private final ObjectMapper objectMapper = BiosObjectMapperProvider.get();
  private final TypeReference<Map<String, String>> paramsType = new TypeReference<>() {};

  protected final HttpClientManager httpClientManager = BiosModules.getHttpClientManager();

  public abstract void register(ServiceRouter dispatcher);

  /**
   * Utility method to get session token from request header fields.
   *
   * @param headers Request headers
   * @return Session token or null
   */
  protected SessionToken getSessionToken(Headers<CharSequence, CharSequence, ?> headers) {
    String token = getAuthToken(headers);
    if (token == null) {
      token = getCookieToken(headers);
      return new SessionToken(token, true);
    }
    return new SessionToken(token, false);
  }

  // TODO(Naoki): Is it better to raise an exception in case of invalid syntax?
  private String getAuthToken(Headers<CharSequence, CharSequence, ?> headers) {
    final var authorization = headers.get(AUTHORIZATION);
    if (authorization == null || authorization.length() <= AUTH_PREFIX.length()) {
      return null;
    }

    final var prefix = authorization.subSequence(0, AUTH_PREFIX.length()).toString();
    if (!AUTH_PREFIX.equalsIgnoreCase(prefix)) {
      return null;
    }
    return authorization.subSequence(AUTH_PREFIX.length(), authorization.length()).toString();
  }

  private String getCookieToken(Headers<CharSequence, CharSequence, ?> headers) {
    final var cookieStrings = headers.getAll(COOKIE);
    if (cookieStrings == null) {
      return null;
    }
    for (var cookieString : cookieStrings) {
      final var cookies = ServerCookieDecoder.STRICT.decode(cookieString.toString());
      for (var cookie : cookies) {
        if (Keywords.TOKEN.equals(cookie.name())) {
          return cookie.value();
        }
      }
    }
    return null;
  }

  protected RequestPhase getRequestPhase(Headers<CharSequence, CharSequence, ?> headers) {
    final var src = headers.get(Keywords.X_BIOS_REQUEST_PHASE);
    if (src == null) {
      return RequestPhase.INITIAL;
    }
    try {
      return RequestPhase.valueOf(src.toString());
    } catch (IllegalArgumentException e) {
      // TODO(Naoki): throw something
      return RequestPhase.INITIAL;
    }
  }

  protected Long determineTimestamp(Headers<CharSequence, CharSequence, ?> headers) {
    final var src = headers.get(Keywords.X_BIOS_TIMESTAMP);
    if (src != null) {
      try {
        return Long.parseLong(src.toString());
      } catch (NumberFormatException e) {
        // TODO(naoki): throw something
      }
    }
    return System.currentTimeMillis();
  }

  /**
   * Utility method to retrieve client version from request header fields
   *
   * @param headers Request headers
   * @return Client version or null
   */
  protected String getClientVersion(Headers<CharSequence, CharSequence, ?> headers) {
    final var cverHeader = headers.get(Keywords.X_BIOS_VERSION);
    return cverHeader != null ? cverHeader.toString() : null;
  }

  /**
   * Help method to get a query parameter as a string
   *
   * @param params Parameters map that can be null
   * @param key Parameter name
   * @param defaultValue Default value, can be null
   * @return The parameter value as a string
   */
  protected String getString(Map<String, String> params, String key, String defaultValue) {
    Objects.requireNonNull(key);
    return params != null ? params.getOrDefault(key, defaultValue) : defaultValue;
  }

  /**
   * Helper method to parse a query parameter as a list delimited by comma.
   *
   * @param params Parameters map that can be null
   * @param key Parameter name
   * @return List of parameters if the parameter src is not null, null otherwise.
   * @throws NullPointerException If specified key is null
   */
  protected List<String> parseQueryParameterList(Map<String, String> params, String key) {
    Objects.requireNonNull(key);
    if (params == null) {
      return null;
    }
    final var src = params.get(key);
    return src != null ? List.of(src.split(",")) : null;
  }

  /**
   * Helper method to parse a query parameter as a boolean.
   *
   * @param params Parameters map that can be null
   * @param key Parameter name
   * @return The parameter value as a boolean
   * @throws NullPointerException If specified key is null
   */
  protected boolean getBoolean(Map<String, String> params, String key) {
    Objects.requireNonNull(key);
    final var src = params != null ? params.get(key) : null;
    return Boolean.parseBoolean(src);
  }

  /**
   * Helper method to parse a query parameter as an integer.
   *
   * @param params Parameters map that can be null
   * @param key Parameter name
   * @param defaultValue Default value used in case the parameter is not specified
   * @return The parameter value as a boolean
   * @throws NullPointerException If specified key is null
   * @throws CompletionException Thrown with InvalidRequestException being wrapped to indicate the
   *     specified parameter is invalid
   */
  protected Integer getInteger(Map<String, String> params, String key, Integer defaultValue) {
    Objects.requireNonNull(key);
    final var src = params != null ? params.get(key) : null;
    if (src == null) {
      return defaultValue;
    }
    try {
      return Integer.valueOf(src);
    } catch (NumberFormatException e) {
      throw new CompletionException(
          new InvalidRequestException(
              String.format("Invalid integer query parameter: ", key, src)));
    }
  }

  /**
   * Helper method to parse a query parameter as a long integer.
   *
   * @param params Parameters map that can be null
   * @param key Parameter name
   * @param defaultValue Default value used in case the parameter is not specified
   * @return The parameter value as a boolean
   * @throws NullPointerException If specified key is null
   * @throws CompletionException Thrown with InvalidRequestException being wrapped to indicate the
   *     specified parameter is invalid
   */
  protected Long getLong(Map<String, String> params, String key, Long defaultValue) {
    Objects.requireNonNull(key);
    final var src = params != null ? params.get(key) : null;
    if (src == null) {
      return defaultValue;
    }
    try {
      return Long.valueOf(src);
    } catch (NumberFormatException e) {
      throw new CompletionException(
          new InvalidRequestException(
              String.format("Invalid integer query parameter: ", key, src)));
    }
  }

  /**
   * Helper method to parse a HTTP2 header parameter as a string.
   *
   * @param headers HTTP2 headers
   * @param key Parameter name
   * @param defaultValue Default value, can be null
   * @return The parameter value as a string
   */
  protected String getString(
      Headers<CharSequence, CharSequence, ?> headers, String key, String defaultValue) {
    Objects.requireNonNull(key);
    final var src = headers != null ? headers.get(key) : null;
    return src != null ? src.toString() : defaultValue;
  }

  /**
   * Helper method to parse a HTTP2 header parameter as a long integer.
   *
   * @param headers HTTP2 headers
   * @param key Parameter name
   * @param defaultValue Default value, can be null
   * @return The parameter value as a long integer
   * @throws InvalidRequestException thrown to indicate that the header parameter value is not
   *     parsable
   */
  protected Long getLong(
      Headers<CharSequence, CharSequence, ?> headers, String key, Long defaultValue)
      throws InvalidRequestException {
    Objects.requireNonNull(key);
    final var src = headers != null ? headers.get(key) : null;
    if (src == null) {
      return defaultValue;
    }
    try {
      return Long.parseLong(src.toString());
    } catch (NumberFormatException ex) {
      throw new InvalidRequestException(
          String.format(
              "Input header parameter %s is required to be Long, but %s is specified (%s)",
              key, src, ex.getMessage()));
    }
  }

  /**
   * Helper method to parse a HTTP2 header parameter as a boolean.
   *
   * @param headers HTTP2 headers
   * @param key Parameter name
   * @param defaultValue Default value
   * @return The parameter value as a boolean
   */
  protected boolean getBoolean(
      Headers<CharSequence, CharSequence, ?> headers, String key, boolean defaultValue) {
    Objects.requireNonNull(key);
    final var src = headers != null ? headers.get(key) : null;
    return src != null ? Boolean.parseBoolean(src.toString()) : defaultValue;
  }

  protected <RequestT, ResponseT> Optional<FanRouter<RequestT, ResponseT>> createFanRouter(
      Headers<CharSequence, CharSequence, ?> headers,
      RequestPhase phase,
      SessionToken sessionToken,
      Class<ResponseT> responseClass) {
    final String path = headers.get(Keywords.PATH).toString();
    final var method = HttpMethod.valueOf(headers.get(Keywords.METHOD).toString());
    return createFanRouter(method, () -> path, phase, sessionToken, responseClass);
  }

  protected <RequestT, ResponseT> Optional<FanRouter<RequestT, ResponseT>> createFanRouter(
      HttpMethod method,
      Supplier<String> pathSupplier,
      RequestPhase phase,
      SessionToken sessionToken,
      Class<ResponseT> responseClass) {
    Objects.requireNonNull(method);
    Objects.requireNonNull(pathSupplier);
    Objects.requireNonNullElse(phase, RequestPhase.INITIAL);
    if (phase == RequestPhase.FINAL) {
      return Optional.empty();
    }
    final var fanRouter =
        new HttpFanRouter<RequestT, ResponseT>(
            method.name(),
            "",
            pathSupplier,
            sessionToken.getToken(),
            httpClientManager,
            responseClass);
    return Optional.of(fanRouter);
  }

  /**
   * Parses a signup service request body into a parameter map.
   *
   * <p>Signup service request parameters would come as a JSON object. This method parses the
   * request and returns a map of string and string.
   *
   * @param paramsSrc Source string
   * @return Parameter map
   * @throws InvalidRequestException thrown to indicate that the request JSON is invalid.
   */
  protected Map<String, String> parseParamsSrc(String paramsSrc) throws InvalidRequestException {
    try {
      return objectMapper.readValue(paramsSrc, paramsType);
    } catch (JsonProcessingException e) {
      logger.warn("Request parsing error", e);
      throw new InvalidRequestException("Invalid request parameters JSON");
    }
  }
}
