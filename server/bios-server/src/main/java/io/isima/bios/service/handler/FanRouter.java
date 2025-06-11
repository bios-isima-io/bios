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
package io.isima.bios.service.handler;

import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutionState;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Fan router interface.
 *
 * @param <RequestT> Request object type
 */
public interface FanRouter<RequestT, ResponseT> {

  /**
   * Method to add an operation parameter.
   *
   * @param name Parameter name
   * @param value Parameter value
   */
  void addParam(String name, String value);

  /**
   * Method to invoke fan routing.
   *
   * <p>This method executes synchronously. The method returns when the operation concludes.
   *
   * <p>The method first finds the server endpoints where the method sends the final phase requests.
   * If no endpoints are registered, the method returns false immediately without running the
   * final-phase operations. The fan routing succeeds when the method receives equal or more than
   * quorum number of success replies, otherwise throws TfosException (TODO: make the exception more
   * precise).
   *
   * <p>If any of the final-phase operations fail due to network errors, the method collects the
   * node information and records failure reports, so that the failed node run the recovery
   * procedure afterwards.
   *
   * @param request The request object. The object must be exactly the same with the one used for
   *     the initial phase operation, thus must be verified already. The value may be null.
   * @param timestamp The request timestamp
   * @return True when the method executes the final-phase operations. False otherwise. The false
   *     case happens when no endpoints are registered.
   * @throws TfosException Thrown to indicate that an user error happens
   * @throws ApplicationException Thrown to indicate that an application error happens
   * @throws NullPointerException when request or timestamp is null
   */
  List<ResponseT> fanRoute(RequestT request, Long timestamp)
      throws TfosException, ApplicationException;

  CompletionStage<List<ResponseT>> fanRouteAsync(
      RequestT request, Long timestamp, ExecutionState state);

  /**
   * Sets the session token.
   *
   * <p>The method replaces the existing token if it is set already.
   *
   * @param sessionToken The new session token
   */
  default void setSessionToken(String sessionToken) {
    // do nothing in default
  }
}
