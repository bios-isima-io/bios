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

import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import io.isima.bios.dto.PasswordSpec;
import io.isima.bios.dto.ServiceRegistrationRequest;
import io.isima.bios.dto.ServiceRegistrationResponse;
import io.isima.bios.dto.SignupRequest;
import io.isima.bios.dto.SignupResponse;
import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.Credentials;
import io.isima.bios.models.InviteRequest;
import io.isima.bios.server.MediaType;
import io.isima.bios.server.ServiceRouter;
import io.isima.bios.service.handler.SignupServiceHandler;
import io.netty.handler.codec.Headers;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.commons.fileupload.MultipartStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SignupBiosService extends BiosService {
  public static final Logger logger = LoggerFactory.getLogger(SignupBiosService.class);

  private final SignupServiceHandler handler = BiosModules.getSignUpServiceHandler();

  @Override
  public void register(ServiceRouter serviceRouter) {
    serviceRouter.addHandler(
        new RestHandler<>(
            "InitiateSignup",
            POST,
            BiosServicePath.PATH_SIGNUP_INITIATE,
            ACCEPTED,
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_JSON,
            SignupRequest.class,
            SignupResponse.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<SignupResponse> handle(
              SignupRequest params,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {

            return handler.executeInSideline(state, () -> handler.initiateSignup(params, state));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "VerifySignupToken",
            POST,
            BiosServicePath.PATH_SIGNUP_VERIFY,
            OK,
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_JSON,
            PasswordSpec.class,
            Map.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<Map> handle(
              PasswordSpec params,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {

            return handler.executeInSideline(state, () -> handler.verifyUser(params, state));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "ApproveUserSignup",
            POST,
            BiosServicePath.PATH_SIGNUP_APPROVE,
            ACCEPTED,
            MediaType.MULTIPART_FORM_DATA,
            MediaType.TEXT_PLAIN,
            String.class,
            String.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<String> handle(
              String body,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {

            final String boundary = getString(headers, ".content-type.boundary", "");

            return handler.executeInSideline(
                state,
                () -> {
                  final var params = parseMultipartBody(body, boundary);
                  return handler.approveUser(params, state);
                });
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "RequestEmailVerification",
            POST,
            BiosServicePath.PATH_SIGNUP_VERIFY_EMAIL,
            OK,
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_JSON,
            Credentials.class,
            Map.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<Map> handle(
              Credentials request,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {

            return handler.executeInSideline(
                state, () -> handler.requestEmailVerification(request, state));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "CompleteSignup",
            POST,
            BiosServicePath.PATH_SIGNUP_COMPLETE,
            OK,
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_JSON,
            PasswordSpec.class,
            Map.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<Map> handle(
              PasswordSpec params,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {

            return handler.executeInSideline(
                state, () -> handler.completeUserSignup(params, state));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "InviteUser",
            POST,
            BiosServicePath.PATH_SIGNUP_INVITE,
            ACCEPTED,
            MediaType.APPLICATION_JSON,
            MediaType.NONE,
            InviteRequest.class,
            Void.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<Void> handle(
              InviteRequest request,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {

            final var sessionToken = getSessionToken(headers);

            return handler.executeInSideline(
                state, () -> handler.inviteUser(sessionToken, request, state));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "RegisterForService",
            POST,
            BiosServicePath.PATH_SERVICE_REGISTRATION,
            OK,
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_JSON,
            ServiceRegistrationRequest.class,
            ServiceRegistrationResponse.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<ServiceRegistrationResponse> handle(
              ServiceRegistrationRequest request,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {

            final var sessionToken = getSessionToken(headers);

            return handler.executeInSideline(
                state, () -> handler.registerForService(sessionToken, request, state));
          }
        });
  }

  private Map<String, String> parseMultipartBody(String body, String boundary)
      throws InvalidRequestException, ApplicationException {
    final var content = new ByteArrayInputStream(body.getBytes());
    try {
      final var multipartStream =
          new MultipartStream(content, boundary.getBytes(), boundary.length() * 2, null);
      boolean nextPart = multipartStream.skipPreamble();
      final var contents = new HashMap<String, String>();
      while (nextPart) {
        final Pair<String, Map<String, String>> disposition =
            getDisposition(multipartStream.readHeaders());
        final var dispositionType = disposition.getLeft();
        final var dispositionParams = disposition.getRight();
        if (!"form-data".equals(dispositionType) || !dispositionParams.containsKey("name")) {
          continue;
        }
        OutputStream output = new ByteArrayOutputStream();
        multipartStream.readBodyData(output);
        contents.put(dispositionParams.get("name"), output.toString());
        nextPart = multipartStream.readBoundary();
      }
      return contents;
    } catch (MultipartStream.MalformedStreamException | IllegalArgumentException e) {
      logger.warn("Malformed input: {}", e);
      throw new InvalidRequestException("Malformed data body");
    } catch (IOException e) {
      throw new ApplicationException("Failed to read request body");
    }
  }

  private Pair<String, Map<String, String>> getDisposition(String headersSrc) throws IOException {
    final var headers = new HashMap<String, String>();
    final var reader = new BufferedReader(new StringReader(headersSrc));
    String header = reader.readLine();
    while (!StringUtils.isBlank(header)) {
      final var keyValue = header.split(":", 2);
      if (keyValue.length == 2 && keyValue[0].trim().equalsIgnoreCase("content-disposition")) {
        var elements = keyValue[1].trim().split(";", 2);
        final var disposition = elements[0];
        final Map<String, String> params;
        if (elements.length > 1) {
          params = new HashMap<>();
          final var paramEntries = elements[1].trim().split(",");
          for (var paramEntry : paramEntries) {
            final var paramKeyValue = paramEntry.trim().split("=", 2);
            if (paramKeyValue.length == 2) {
              String value = paramKeyValue[1].trim();
              if (value.startsWith("\"") && value.endsWith("\"")) {
                value = value.substring(1, value.length() - 1);
              }
              params.put(paramKeyValue[0].trim().toLowerCase(), value);
            }
          }
        } else {
          params = Map.of();
        }
        return Pair.of(disposition, params);
      }
    }
    return Pair.of("", Map.of());
  }
}
