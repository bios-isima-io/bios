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

import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.audit.AuditConstants;
import io.isima.bios.audit.AuditManager;
import io.isima.bios.audit.AuditOperation;
import io.isima.bios.audit.OperationStatus;
import io.isima.bios.auth.AllowedRoles;
import io.isima.bios.auth.Auth;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.data.DataEngine;
import io.isima.bios.dto.GetUsersResponse;
import io.isima.bios.errors.GenericError;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.errors.exception.PermissionDeniedException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.errors.exception.UserAccessControlException;
import io.isima.bios.exceptions.validator.ConstraintViolationValidatorException;
import io.isima.bios.exceptions.validator.MultipleValidatorViolationsException;
import io.isima.bios.exceptions.validator.ValidatorException;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.mail.Email;
import io.isima.bios.mail.MailClient;
import io.isima.bios.metrics.OperationMetrics;
import io.isima.bios.models.Role;
import io.isima.bios.models.SessionToken;
import io.isima.bios.models.UserConfig;
import io.isima.bios.models.UserContext;
import io.isima.bios.models.auth.User;
import io.isima.bios.models.v1.Permission;
import io.isima.bios.repository.auth.UserRepository;
import io.isima.bios.user.UserManager;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserManagementServiceHandler extends ServiceHandler {
  private static final Logger logger = LoggerFactory.getLogger(UserManagementServiceHandler.class);

  private final UserRepository userRepository;
  private final UserManager userManager;
  private final AuditManager auditManager;
  private final MailClient mailClient;
  private final Auth auth;

  public UserManagementServiceHandler(
      UserRepository userRepository,
      UserManager userManager,
      Auth auth,
      AdminInternal admin,
      DataEngine dataEngine,
      OperationMetrics metrics,
      AuditManager auditManager,
      MailClient mailClient) {
    super(auditManager, auth, admin, dataEngine, metrics);
    this.userRepository = userRepository;
    this.userManager = userManager;
    this.auditManager = auditManager;
    this.mailClient = mailClient;
    this.auth = auth;
  }

  /**
   * Creates a user.
   *
   * @return Completable future for the created user.
   */
  public CompletableFuture<UserConfig> createUser(
      SessionToken sessionToken, UserConfig userConfig, GenericExecutionState state) {
    final var auditInfo =
        auditManager.begin(AuditConstants.NA, AuditOperation.CREATE_USER, userConfig.toString());
    return CompletableFuture.supplyAsync(
            () -> {
              try {
                final var userContext =
                    validateTokenCore(sessionToken, "", AllowedRoles.SYSADMIN, state);
                if (userConfig.getTenantName() == null) {
                  throw new CompletionException(
                      new InvalidRequestException(
                          "Tenant name must be set to create a user by a system admin user"));
                }
                return userContext;
              } catch (UserAccessControlException e) {
                try {
                  final var userContext =
                      validateTokenCore(sessionToken, "", AllowedRoles.TENANT_WRITE, state);
                  if (userConfig.getTenantName() != null
                      && !userContext.getTenant().equalsIgnoreCase(userConfig.getTenantName())) {
                    throw new PermissionDeniedException();
                  }
                  return userContext;
                } catch (UserAccessControlException e2) {
                  throw new CompletionException(e2);
                }
              }
            },
            state.getExecutor())
        .thenAccept(
            (userContext) -> {
              state.setUserContext(userContext);
              if (userConfig.getTenantName() == null) {
                userConfig.setTenantName(userContext.getTenant());
              }
              auditInfo.setUser(userContext.getSubject());
              auditInfo.setTenant(userContext.getTenant());
              auditInfo.setUserId(userContext.getUserId());
              auditInfo.setOrganizationId(userContext.getOrgId());
              validateCreateUserRequest(userConfig);
            })
        .thenCompose((none) -> userManager.createUserAsync(userConfig, state))
        .thenApply(
            (newUser) -> {
              final var newUserConfig = newUser.toUserConfig();
              newUserConfig.setTenantName(userConfig.getTenantName());
              auditManager.commit(
                  auditInfo, OperationStatus.SUCCESS.name(), newUserConfig.toString());
              return newUserConfig;
            })
        .exceptionally(
            (t) -> {
              final boolean isCe = (t instanceof CompletionException);
              final var cause = isCe ? t.getCause() : t;
              auditManager.commit(auditInfo, OperationStatus.FAILURE.name(), cause.getMessage());
              throw isCe ? (CompletionException) t : new CompletionException(cause);
            });
  }

  /**
   * Gets users in a tenant.
   *
   * @return Users information
   */
  public CompletableFuture<GetUsersResponse> getUsers(
      SessionToken sessionToken, String email, GenericExecutionState state) {
    return CompletableFuture.supplyAsync(
            () -> {
              try {
                return validateTokenCore(sessionToken, "", AllowedRoles.TENANT_READ, state);
              } catch (UserAccessControlException e) {
                throw new CompletionException(e);
              }
            },
            state.getExecutor())
        .thenCompose(
            (userContext) -> {
              List<String> userEmails = null;
              if (email != null) {
                userEmails = List.of(email.toLowerCase().split(","));
              }
              if (userEmails == null) {
                return getUsersInOrg(userContext.getOrgId(), userEmails, state);
              } else {
                return getUsersWithEmails(userEmails, state);
              }
            });
  }

  private CompletionStage<GetUsersResponse> getUsersInOrg(
      Long orgId, List<String> userEmails, ExecutionState state) {
    return userRepository
        .findAllByOrg(orgId, null, state)
        .thenApply(
            (users) -> {
              return new GetUsersResponse(
                  users.stream()
                      .filter(
                          (user) ->
                              userEmails == null
                                  ? true
                                  : userEmails.contains(user.getEmail().toLowerCase()))
                      .map((user) -> userToUserConfig(user))
                      .collect(Collectors.toList()));
            });
  }

  private CompletionStage<GetUsersResponse> getUsersWithEmails(
      List<String> userEmails, ExecutionState state) {
    final var users = new UserConfig[userEmails.size()];
    final var futures = new CompletableFuture[userEmails.size()];
    for (int i = 0; i < userEmails.size(); ++i) {
      final int requestIndex = i;
      futures[i] =
          userRepository
              .findByEmail(userEmails.get(requestIndex), state)
              .thenAccept(
                  (user) -> {
                    users[requestIndex] = userToUserConfig(user);
                  });
    }
    return CompletableFuture.allOf(futures)
        .thenApply((none) -> new GetUsersResponse(List.of(users)));
  }

  private UserConfig userToUserConfig(User user) {
    final var userConfig = new UserConfig();
    if (user.getId() != null) {
      userConfig.setUserId(user.getId().toString());
    } else {
      logger.warn("Invalid user found, user ID is missing; email={}", user.getEmail());
    }
    userConfig.setEmail(user.getEmail());
    userConfig.setFullName(user.getName());
    userConfig.setRoles(
        Permission.getByIds(Permission.parse(user.getPermissions())).stream()
            .map((permission) -> permission.toBios())
            .collect(Collectors.toList()));
    userConfig.setStatus(user.getStatus());
    userConfig.setModifyTimestamp(
        user.getUpdatedAt() != null ? user.getUpdatedAt().getTime() : null);
    return userConfig;
  }

  /**
   * Modify a user.
   *
   * @param userId User ID
   * @param userConfig User properties to modify
   * @return Future for the configuration of the modified user
   */
  public CompletableFuture<UserConfig> modifyUser(
      SessionToken sessionToken, Long userId, UserConfig userConfig, GenericExecutionState state) {
    final var auditInfo =
        auditManager.begin(
            AuditConstants.NA,
            AuditOperation.MODIFY_USER,
            "userId=" + userId + ", params=" + userConfig);
    return CompletableFuture.supplyAsync(
            () -> {
              try {
                return validateTokenCore(sessionToken, "", AllowedRoles.TENANT_READ, state);
              } catch (UserAccessControlException e) {
                throw new CompletionException(e);
              }
            },
            state.getExecutor())
        .thenCompose(
            (userContext) -> {
              state.setUserContext(userContext);
              auditInfo.setUser(userContext.getSubject());
              auditInfo.setTenant(userContext.getTenant());
              auditInfo.setUserId(userContext.getUserId());
              auditInfo.setOrganizationId(userContext.getOrgId());
              if (userConfig.getRoles() != null
                  && userConfig.getRoles().contains(Role.INTERNAL)
                  && !userContext.getPermissions().contains(Permission.SUPERADMIN)) {
                throw new CompletionException(
                    new PermissionDeniedException(
                        userContext, "Only system administrator can add internal user role"));
              }
              return userManager.modifyUserAsync(userId, userConfig, userContext, state);
            })
        .thenApply(
            (newUserConfig) -> {
              auditManager.commit(
                  auditInfo, OperationStatus.SUCCESS.name(), newUserConfig.toString());
              return newUserConfig;
            });
  }

  /**
   * Delete a user.
   *
   * <p>Either of userId or email must be set to specify the user.
   *
   * @param email Email address of the user to delete
   * @param userId User ID
   * @return Future for the completion, no return value
   */
  public CompletableFuture<Void> deleteUser(
      SessionToken sessionToken, String email, Long userId, GenericExecutionState state) {
    if (email == null && userId == null) {
      return CompletableFuture.failedFuture(
          new TfosException(
              GenericError.INVALID_REQUEST,
              "Either of query parameters 'email' or 'userId' must be set"));
    }
    final String target;
    if (email != null) {
      target = "email=" + email;
    } else {
      target = "userId=" + userId;
    }
    final var auditInfo = auditManager.begin(AuditConstants.NA, AuditOperation.DELETE_USER, target);
    return CompletableFuture.supplyAsync(
            () -> {
              try {
                return validateTokenCore(sessionToken, "", AllowedRoles.TENANT_READ, state);
              } catch (UserAccessControlException e) {
                throw new CompletionException(e);
              }
            },
            state.getExecutor())
        .thenCompose(
            (userContext) -> {
              auditInfo.setUser(userContext.getSubject());
              auditInfo.setTenant(userContext.getTenant());
              auditInfo.setUserId(userContext.getUserId());
              auditInfo.setOrganizationId(userContext.getOrgId());
              return userManager.deleteUserAsync(email, userId, userContext, state);
            })
        .thenRun(() -> auditManager.commit(auditInfo, OperationStatus.SUCCESS.name(), ""))
        .exceptionally(
            (t) -> {
              final boolean isCe = (t instanceof CompletionException);
              final var cause = isCe ? t.getCause() : t;
              auditManager.commit(auditInfo, OperationStatus.FAILURE.name(), cause.getMessage());
              throw isCe ? (CompletionException) t : new CompletionException(cause);
            });
  }

  private void validateCreateUserRequest(UserConfig userConfig) {
    final var errors = new ArrayList<ValidatorException>();
    if (userConfig.getEmail() == null) {
      errors.add(new ConstraintViolationValidatorException("email must be set"));
    }
    if (userConfig.getTenantName() == null) {
      errors.add(new ConstraintViolationValidatorException("tenantName must be set"));
    }
    if (userConfig.getRoles() == null || userConfig.getRoles().isEmpty()) {
      errors.add(new ConstraintViolationValidatorException("roles must be set"));
    }
    if (userConfig.getPassword() == null) {
      errors.add(new ConstraintViolationValidatorException("password must be set"));
    }
    if (userConfig.getStatus() == null) {
      errors.add(new ConstraintViolationValidatorException("status must be set"));
    }
    TfosException error = null;
    if (errors.size() > 1) {
      error = new ConstraintViolationException(new MultipleValidatorViolationsException(errors));
    } else if (errors.size() == 1) {
      error = new ConstraintViolationException(errors.get(0));
    }
    if (error != null) {
      throw new CompletionException(error);
    }
  }

  private void sendInviteEmail(UserContext userContext) {
    long currentTime = System.currentTimeMillis();
    long expirationTime = currentTime + TfosConfig.signupExpirationTimeMillis();
    final String token = auth.createToken(currentTime, expirationTime, userContext);
    final String verifyUrlFormat = TfosConfig.invitationVerifyBaseUrl();
    final String verifyUrl = String.format(verifyUrlFormat, token);
    final String mailContent = String.format(TfosConfig.signupMailContent(), verifyUrl);
    final String body;
    final String lineSeparator = System.getProperty("line.separator");

    if (StringUtils.isNotBlank(lineSeparator)) {
      body = String.join(lineSeparator, StringUtils.replace(mailContent, "\n", lineSeparator));
    } else {
      body = mailContent;
    }

    List<String> bccList = TfosConfig.signupMailBccList();

    final Email emailObj =
        Email.builder()
            .subject("Welcome to bi(OS)")
            .textHtml(body)
            .to(userContext.getSubject())
            .bcc(bccList)
            .from(TfosConfig.mailServiceFromAddress())
            .build();
    mailClient.sendMail(emailObj);
  }
}
