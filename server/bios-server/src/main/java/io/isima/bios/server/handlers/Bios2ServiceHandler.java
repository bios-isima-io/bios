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
package io.isima.bios.server.handlers;

import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.data.DataEngine;
import io.isima.bios.errors.exception.AtomicCommitFailedException;
import io.isima.bios.errors.exception.AtomicOperationAbortedException;
import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.errors.exception.PermissionDeniedException;
import io.isima.bios.exceptions.InsertBulkFailedServerException;
import io.isima.bios.execution.AtomicOperationContext;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.AtomicOperationSpec;
import io.isima.bios.models.SessionToken;
import io.isima.bios.models.UserContext;
import io.isima.bios.models.v1.Permission;
import io.isima.bios.service.handler.DataServiceUtils;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

public class Bios2ServiceHandler {

  // protected final ExecutorConfig executorConfig;
  protected final SessionUtils sessionUtils;

  protected final DataEngine dataEngine;
  protected final AdminInternal admin;
  protected final ConcurrentMap<UUID, AtomicOperationContext> atomicOperationContexts;

  public Bios2ServiceHandler(DataEngine dataEngine, AdminInternal admin) {
    this.sessionUtils = new SessionUtils();
    this.dataEngine = dataEngine;
    this.admin = admin;
    this.atomicOperationContexts = new ConcurrentHashMap<>();
  }

  /**
   * Method to validate a session token.
   *
   * @param sessionToken The session token to validate
   * @param tenantName If specified, the method checks whether the tenant name matches the user's
   *     tenant. If the parameter is omitted (i.e., null is specified), the method skips the tenant
   *     validation.
   * @param permittedRoles List of permitted roles for the operation.
   * @param state Execution state
   * @return Future state on validation completion
   * @throws CompletionException that wraps UserAccessControlException thrown to indicate that the
   *     token validation has failed.
   */
  protected void validateToken(
      SessionToken sessionToken,
      String tenantName,
      List<Permission> permittedRoles,
      ExecutionState state,
      Consumer<UserContext> acceptor,
      Consumer<Throwable> errorHandler) {
    state.addHistory("(validateToken");
    sessionUtils.validateSessionToken(
        sessionToken,
        tenantName,
        permittedRoles,
        state,
        (userContext) -> {
          state.setUserContext(userContext);
          state.addHistory(")");
          acceptor.accept(userContext);
        },
        errorHandler::accept);
  }

  /**
   * Method to validate an session token.
   *
   * @param sessionToken The session token to validate
   * @param tenantName If specified, the method checks whether the tenant name matches the user's
   *     tenant. If the parameter is omitted (i.e., null is specified), the method skips the tenant
   *     validation.
   * @param permittedRoles List of permitted roles for the operation.
   * @param state Execution state
   * @return Future state on validation completion
   * @throws CompletionException that wraps UserAccessControlException thrown to indicate that the
   *     token validation has failed.
   */
  protected <StateT extends ExecutionState> CompletableFuture<StateT> validateTokenAsync(
      SessionToken sessionToken, String tenantName, List<Permission> permittedRoles, StateT state) {
    final var future = new CompletableFuture<StateT>();
    state.addHistory("(validateToken");
    sessionUtils.validateSessionToken(
        sessionToken,
        tenantName,
        permittedRoles,
        state,
        (userContext) -> {
          state.setUserContext(userContext);
          state.addHistory(")");
          future.complete(state);
        },
        future::completeExceptionally);
    return future;
  }

  /**
   * Validates a token with allowing delegation.
   *
   * <p>The method allows the operation either if the user meets the regular permission -- the user
   * belongs to the target tenant and has a permitted role, or delegation condition. Delegation is
   * allowed when the user has AppMaster role and the target domain's or tenant's master is the
   * user's tenant.
   *
   * <p>If a domain name is specified in place of tenant for delegation, the method overwrites it by
   * the corresponding tenant name if the session token is valid.
   *
   * @param sessionToken The session token
   * @param tenantName Target tenant or domain name in the request
   * @param permittedRoles Permitted roles for the operation
   * @param state Execution state
   * @return Completable future with updated state
   */
  protected <StateT extends ExecutionState> CompletableFuture<StateT> validateTokenAllowDelegation(
      SessionToken sessionToken, String tenantName, List<Permission> permittedRoles, StateT state) {
    state.addHistory("(validateToken");
    return sessionUtils
        .validateSessionTokenAllowDelegate(sessionToken, tenantName, permittedRoles, state)
        .thenApply(
            (userContext) -> {
              state.setUserContext(userContext);
              state.addHistory(")");
              String resolvedTenant = tenantName;
              if (userContext.getIsAppMaster() == Boolean.TRUE) {
                var tenantConfig = admin.getTenantOrNull(resolvedTenant);
                if (tenantConfig == null) {
                  resolvedTenant = BiosModules.getDomainResolver().getTenantByDomain(tenantName);
                  if (resolvedTenant != null) {
                    tenantConfig = admin.getTenantOrNull(resolvedTenant);
                  }
                }
                if (tenantConfig == null) {
                  throw new CompletionException(new NoSuchTenantException(tenantName));
                }
                if (!userContext.getTenant().equalsIgnoreCase(tenantConfig.getAppMaster())) {
                  throw new CompletionException(
                      new PermissionDeniedException(
                          userContext,
                          String.format(
                              "Target tenant %s does not belong to app master tenant %s",
                              resolvedTenant, userContext.getTenant())));
                }
                state.setTenantName(resolvedTenant);
              }
              return state;
            });
  }

  /**
   * Generic method to verify an instance is not null.
   *
   * @param <T> Type of the target instance
   * @param target The instance to test
   * @param errorMessageFormat Format used for building the error message in case of failure
   * @param errorMessageParams Error message parameters
   * @return The same object with target
   * @throws CompletionException with InvalieRequestException as the cause, thrown to indicate the
   *     target is null.
   */
  protected <T> T verifyNotNull(T target, String errorMessageFormat, Object... errorMessageParams) {
    if (target == null) {
      throw new CompletionException(
          new InvalidRequestException(String.format(errorMessageFormat, errorMessageParams)));
    }
    return target;
  }

  /** Mark execution state an error and rethrow. */
  protected <T> T handleAsyncError(Throwable t, ExecutionState state) {
    state.markError();
    final var cause = t instanceof CompletionException ? t.getCause() : t;
    if (!(cause instanceof InsertBulkFailedServerException)
        || !((InsertBulkFailedServerException) cause).isPartialFailure()) {
      insertOperationFailureEvents(state, cause);
    }
    if (t instanceof CompletionException) {
      throw (CompletionException) t;
    } else {
      throw new CompletionException(t);
    }
  }

  protected void insertOperationFailureEvents(ExecutionState state, Throwable t) {
    String tenantName = state.getTenantName();
    String streamName = state.getStreamName() != null ? state.getStreamName() : "";
    final var streamDesc = state.getStreamDesc();
    if (streamDesc != null) {
      tenantName = streamDesc.getParent().getName();
      streamName = streamDesc.getName();
    }
    final String request =
        state.getRequestType() != null
            ? state.getRequestType().getRequestName()
            : state.getExecutionName();
    final var appType =
        state.getUserContext() != null ? state.getUserContext().getAppType().name() : "";
    final var appName = state.getUserContext() != null ? state.getUserContext().getAppName() : "";
    DataServiceUtils.insertOperationFailureEvents(
        tenantName, streamName, request, appType, appName, t, dataEngine, admin, state);
  }

  protected <StateT extends ExecutionState> StateT setUpAtomicOperation(
      AtomicOperationSpec spec, StateT state) {
    // force enabling atomic operation
    if (spec == null && writeTimeIndexingEnabled(state.getStreamDesc())) {
      spec = new AtomicOperationSpec(UUID.randomUUID(), 1);
    }
    if (spec != null) {
      final var id = spec.getAtomicOperationId();
      final int numBundledRequests = spec.getNumBundledRequests();

      final AtomicOperationContext context =
          atomicOperationContexts.computeIfAbsent(
              id, (i) -> new AtomicOperationContext(i, numBundledRequests));

      state.setAtomicOperationContext(context);

      if (context.startEachOperation()) {
        atomicOperationContexts.remove(id);
      }
    }
    return state;
  }

  private boolean writeTimeIndexingEnabled(StreamDesc streamDesc) {
    if (streamDesc == null || streamDesc.getViews() == null) {
      return false;
    }
    for (var viewDesc : streamDesc.getViews()) {
      if (viewDesc.getWriteTimeIndexing() == Boolean.TRUE) {
        return true;
      }
    }
    return false;
  }

  protected CompletionStage<Void> completeAtomicOperationIfEnabled(ExecutionState state) {
    final var atomicOperationContext = state.getAtomicOperationContext();
    state.setAtomicOperationContext(null);
    if (atomicOperationContext == null) {
      return CompletableFuture.completedStage(null);
    }

    final var pending = new CompletableFuture<Void>();
    if (atomicOperationContext.completeOperation(pending)) {
      if (atomicOperationContext.getFailed().get()) {
        final var error = new AtomicOperationAbortedException();
        atomicOperationContext
            .getPendingCompletions()
            .forEach((fut) -> fut.completeExceptionally(error));
      } else {
        dataEngine
            .commit(atomicOperationContext, state)
            .thenRun(
                () -> {
                  atomicOperationContext
                      .getPendingCompletions()
                      .forEach((fut) -> fut.complete(null));
                })
            .exceptionally(
                (t) -> {
                  final var cause = t instanceof CompletionException ? t.getCause() : t;
                  final var error = new AtomicCommitFailedException(cause);
                  atomicOperationContext
                      .getPendingCompletions()
                      .forEach((fut) -> fut.completeExceptionally(error));
                  return null;
                });
      }
    }
    return pending;
  }

  protected void abortAtomicOperationIfEnabled(ExecutionState state) {
    final var atomicOperationContext = state.getAtomicOperationContext();
    state.setAtomicOperationContext(null);
    if (atomicOperationContext != null && atomicOperationContext.failOperation()) {
      final var error = new AtomicOperationAbortedException();
      atomicOperationContext
          .getPendingCompletions()
          .forEach((fut) -> fut.completeExceptionally(error));
    }
  }
}
