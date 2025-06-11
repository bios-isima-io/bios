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

import static io.isima.bios.admin.v1.AdminConstants.CONTEXT_AUDIT_SIGNAL_PREFIX;
import static io.isima.bios.common.EmailUtil.validateEmail;
import static io.isima.bios.common.TfosConfig.biosDomainName;
import static io.isima.bios.common.TfosConfig.signupApprovalAdminMailId;
import static io.isima.bios.common.TfosConfig.signupApprovalCompetitorAdminMailId;
import static io.isima.bios.common.TfosConfig.signupApprovalMailSubjectTemplateTenant;
import static io.isima.bios.common.TfosConfig.signupApprovalMailSubjectTemplateUser;
import static io.isima.bios.common.TfosConfig.signupAttemptNotificationSubjectTemplate;
import static io.isima.bios.common.TfosConfig.signupRejectedAdminBodyTemplate;
import static io.isima.bios.common.TfosConfig.signupRejectedAdminSubjectTemplate;
import static io.isima.bios.common.TfosConfig.signupWarningAdminBodyTemplate;
import static io.isima.bios.common.TfosConfig.signupWarningAdminSubjectTemplate;
import static io.isima.bios.models.v1.Permission.ADMIN;
import static io.isima.bios.models.v1.Permission.BI_REPORT;
import static io.isima.bios.server.services.BiosServicePath.PATH_CONTEXTS;
import static io.isima.bios.server.services.BiosServicePath.PATH_SIGNALS;
import static io.isima.bios.server.services.BiosServicePath.PATH_TENANTS;
import static io.isima.bios.service.Keywords.PATCH;
import static io.isima.bios.service.Keywords.POST;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.uuid.Generators;
import com.ibm.asyncutil.iteration.AsyncTrampoline;
import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.audit.AuditConstants;
import io.isima.bios.audit.AuditInfo;
import io.isima.bios.audit.AuditManager;
import io.isima.bios.audit.AuditOperation;
import io.isima.bios.auth.AllowedRoles;
import io.isima.bios.bi.Reports;
import io.isima.bios.bi.ReportsConfig;
import io.isima.bios.common.BiosConstants;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.data.DataEngine;
import io.isima.bios.data.UpdateContextEntrySpec;
import io.isima.bios.dto.AttributeSpec;
import io.isima.bios.dto.PasswordSpec;
import io.isima.bios.dto.PutContextEntriesRequest;
import io.isima.bios.dto.ServiceRegistrationRequest;
import io.isima.bios.dto.ServiceRegistrationResponse;
import io.isima.bios.dto.SignupRequest;
import io.isima.bios.dto.SignupResponse;
import io.isima.bios.dto.UpdateContextEntryRequest;
import io.isima.bios.errors.SignupError;
import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.errors.exception.NoSuchUserException;
import io.isima.bios.errors.exception.TenantAlreadyExistsException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.errors.exception.UserAccessControlException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ContextOpState;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.ExecutorManager;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.mail.Email;
import io.isima.bios.mail.MailClient;
import io.isima.bios.maintenance.WorkerLock;
import io.isima.bios.metrics.OperationMetrics;
import io.isima.bios.models.AppType;
import io.isima.bios.models.ContentRepresentation;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.Credentials;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.models.InviteRequest;
import io.isima.bios.models.MemberStatus;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.Role;
import io.isima.bios.models.Service;
import io.isima.bios.models.ServiceType;
import io.isima.bios.models.SessionToken;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.SignupApprovalAction;
import io.isima.bios.models.TenantConfig;
import io.isima.bios.models.TenantData;
import io.isima.bios.models.UserContext;
import io.isima.bios.models.auth.DomainApprovalAction;
import io.isima.bios.models.auth.Organization;
import io.isima.bios.models.auth.User;
import io.isima.bios.models.v1.Permission;
import io.isima.bios.repository.auth.OrganizationRepository;
import io.isima.bios.repository.auth.UserRepository;
import io.isima.bios.server.handlers.ContextWriteOpState;
import io.isima.bios.server.services.BiosServicePath;
import io.isima.bios.service.HttpClientManager;
import io.isima.bios.service.HttpFanRouter;
import io.isima.bios.service.JwtTokenUtils;
import io.isima.bios.service.Keywords;
import io.isima.bios.user.TfosUserManager;
import io.isima.bios.utils.BiosObjectMapperProvider;
import io.isima.bios.utils.Utils;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SignupServiceHandler extends ServiceHandler {
  private static final Logger logger = LoggerFactory.getLogger(SignupServiceHandler.class);
  private static final ObjectMapper mapper = BiosObjectMapperProvider.get();

  private static final String passwordChars =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789~!@#$%^&*+?";

  private final List<Permission> tenantAdminRole;
  private final List<Permission> mlEngineerRole;

  private final UserRepository userRepository;
  private final OrganizationRepository organizationRepository;
  private final MailClient mailClient;
  private final AdminServiceHandler adminServiceHandler;
  private final HttpClientManager clientManager;
  private final WorkerLock workerLock;
  private final Reports reports;

  private final String passwordResetSecret;
  private final String approvalSecret;
  private final String verificationSecret;
  private final String finalizationSecret;

  public SignupServiceHandler(
      MailClient mailClient,
      UserRepository userRepository,
      OrganizationRepository organizationRepository,
      AuditManager auditManager,
      io.isima.bios.auth.Auth auth,
      AdminInternal admin,
      DataEngine dataEngine,
      OperationMetrics metrics,
      AdminServiceHandler adminServiceHandler,
      HttpClientManager clientManager,
      WorkerLock workerLock,
      Reports reports) {

    super(auditManager, auth, admin, dataEngine, metrics);
    this.userRepository = userRepository;
    this.organizationRepository = organizationRepository;
    this.mailClient = mailClient;
    this.adminServiceHandler = adminServiceHandler;

    this.tenantAdminRole = List.of(ADMIN, BI_REPORT);
    this.mlEngineerRole = List.of(BI_REPORT, Permission.INGEST, Permission.EXTRACT);
    this.clientManager = clientManager;
    this.workerLock = workerLock;
    this.reports = reports;
    passwordResetSecret = TfosConfig.passwordResetTokenSecret();
    approvalSecret = TfosConfig.approvalTokenSecret();
    verificationSecret = TfosConfig.verificationTokenSecret();
    finalizationSecret = TfosConfig.finalizationTokenSecret();
  }

  /**
   * Method handles forgot password. Sends password reset link to user through email.
   *
   * @param params Request parameters
   */
  public CompletableFuture<Void> initiatePasswordReset(SignupRequest params, ExecutionState state) {

    final AuditInfo auditInfo;
    try {
      auditInfo =
          auditManager.begin(
              AuditConstants.NA, AuditOperation.APPROVE_USER, mapper.writeValueAsString(params));
    } catch (JsonProcessingException e) {
      return CompletableFuture.failedFuture(
          new ApplicationException("Failed to create audit info", e));
    }

    final var email = params.getEmail();
    if (email == null) {
      final var e = new InvalidRequestException("Parameter 'email' is missing");
      auditManager.commit(auditInfo, e);
      return CompletableFuture.failedFuture(e);
    }
    auditInfo.setUser(email);

    return userRepository
        .findByEmail(email, state)
        .thenCompose(
            (user) -> {
              if (user == null) {
                return CompletableFuture.failedFuture(
                    new InvalidRequestException("User not found"));
              }
              return processForgotPasswordRequest(email, user, state);
            })
        .whenCompleteAsync(
            (r, t) -> {
              commitAuditLog(r, t, auditInfo);
              if (t != null) {
                Throwable cause = t.getCause() != null ? t.getCause() : t;
                logger.warn("failed generate link to reset password; reason={}", cause.toString());
              }
            });
  }

  private CompletableFuture<Void> processForgotPasswordRequest(
      String emailId, User user, ExecutionState state) {
    long currentTime = System.currentTimeMillis();
    long expirationTime = currentTime + TfosConfig.signupExpirationTimeMillis();

    UserContext newContext = new UserContext();
    newContext.setSubject(auth.hash(emailId));
    newContext.setUserId(user.getId());
    newContext.setOrgId(user.getOrgId());

    String token =
        JwtTokenUtils.createToken(currentTime, expirationTime, newContext, passwordResetSecret);

    // send password reset email asynchronously (i.e., we don't wait for the result)
    return CompletableFuture.runAsync(
        () -> {
          sendPasswordResetEmail(emailId, token, user.getName());
          logger.info("Sent password reset email; recipient={}, user={}", emailId, user.getName());
        },
        state.getExecutor());
  }

  /**
   * Method to set password using the password reset token.
   *
   * @param params Request parameters
   */
  public CompletableFuture<String> resetPassword(PasswordSpec params, ExecutionState state) {
    final AuditInfo auditInfo;
    try {
      auditInfo =
          auditManager.begin(
              AuditConstants.NA, AuditOperation.APPROVE_USER, mapper.writeValueAsString(params));
    } catch (JsonProcessingException e) {
      return CompletableFuture.failedFuture(
          new ApplicationException("Failed to create audit info", e));
    }

    try {
      final String password = params.getPassword();
      if (password == null || password.isEmpty()) {
        final var e = new InvalidRequestException("Parameter 'password' is not set");
        auditManager.commit(auditInfo, e);
        return CompletableFuture.failedFuture(e);
      }

      final String token = params.getToken();

      final var userFuture = new CompletableFuture<UserContext>();
      JwtTokenUtils.parseToken(
          token,
          passwordResetSecret,
          false,
          state,
          userFuture::complete,
          userFuture::completeExceptionally);

      return userFuture
          .thenComposeAsync(
              (userContext) -> {
                auditInfo.setUserContext(userContext);
                return userRepository
                    .findById(userContext.getOrgId(), userContext.getUserId(), state)
                    .thenCompose(
                        user -> {
                          if (user == null) {
                            logger.warn(
                                "Resetting password failed since the user was not found;"
                                    + " userId={}, organizationId={}",
                                userContext.getUserId(),
                                userContext.getOrgId());
                            throw new CompletionException(
                                new NoSuchUserException("User not found"));
                          }
                          Date date = new Date();
                          user.setUpdatedAt(date);
                          user.setPassword(auth.hash(password));
                          return userRepository.save(user, state);
                        })
                    .thenApply((user) -> "Success");
              },
              state.getExecutor())
          .thenApply((user) -> "Success")
          .whenCompleteAsync((r, t) -> commitAuditLog(r, t, auditInfo), state.getExecutor());
    } catch (Throwable t) {
      auditManager.commit(auditInfo, t);
      return CompletableFuture.failedFuture(t);
    }
  }

  private String getPasswordResetToken(User user, String email) {
    long currentTime = System.currentTimeMillis();
    long expirationTime = currentTime + TfosConfig.signupExpirationTimeMillis();
    UserContext newContext = new UserContext();
    newContext.setSubject(auth.hash(email));
    newContext.setUserId(user.getId());
    newContext.setOrgId(user.getOrgId());
    return JwtTokenUtils.createToken(currentTime, expirationTime, newContext, passwordResetSecret);
  }

  /**
   * Method to generate a token used for signup approval.
   *
   * @param request Request parameters
   */
  public CompletableFuture<SignupResponse> initiateSignup(
      SignupRequest request, ExecutionState state) {

    final AuditInfo auditInfo;
    try {
      auditInfo =
          auditManager.begin(
              AuditConstants.NA,
              AuditOperation.INITIATE_SIGNUP,
              mapper.writeValueAsString(request));
    } catch (JsonProcessingException e) {
      return CompletableFuture.failedFuture(
          new ApplicationException("Failed to create audit info", e));
    }

    String email = request.getEmail();
    if (!validateEmail(email)) {
      final var e = new TfosException(SignupError.INVALID_EMAIL_ADDRESS, email);
      auditManager.commit(auditInfo, e);
      return CompletableFuture.failedFuture(e);
    }
    auditInfo.setUser(email);

    if (request.getAdditionalUsers() != null) {
      for (var additionalEmail : request.getAdditionalUsers()) {
        if (!validateEmail(additionalEmail)) {
          final var e = new TfosException(SignupError.INVALID_EMAIL_ADDRESS, additionalEmail);
          auditManager.commit(auditInfo, e);
          return CompletableFuture.failedFuture(e);
        }
      }
    }

    String[] tokens = email.split("@");
    String primaryEmailDomain = tokens[1];

    final var organizationName =
        ObjectUtils.firstNonNull(request.getDomain(), getOrganizationName(primaryEmailDomain));
    final var tenantName = organizationName.replace(".", "_");

    return userRepository
        .findByEmail(email, state)
        .thenCombine(
            organizationRepository.findByTenantName(tenantName, state),
            (user, org) -> {
              if (user != null) {
                if (org == null || !user.getOrgId().equals(org.getId())) {
                  throw new CompletionException(new TfosException(SignupError.INVALID_DOMAIN));
                }
                if (!BiosModules.isTestMode()) {
                  String passwordResetToken = getPasswordResetToken(user, email);
                  sendAlreadySignedUpEmail(email, passwordResetToken, user.getName());
                  // ACCEPTED
                  sendSignupAttemptNotificationToAdministrator(email, tenantName);
                }
                return false;
              }
              return true;
            })
        .thenComposeAsync(
            (isNewUser) ->
                isNewUser
                    ? processSignup(
                        email, primaryEmailDomain, request, organizationName, tenantName, state)
                    : CompletableFuture.completedFuture(null),
            state.getExecutor())
        .whenCompleteAsync((r, t) -> commitAuditLog(r, t, auditInfo), state.getExecutor());
  }

  private CompletableFuture<SignupResponse> processSignup(
      String email,
      String primaryEmailDomain,
      SignupRequest request,
      String organizationName,
      String tenantName,
      ExecutionState state) {
    final var poster = state.getExecutor();

    final var future = new CompletableFuture<DomainApprovalAction>();

    final Service service;
    if (request.getService() != null) {
      try {
        service = verifyServiceRegistrationRequest(request);
      } catch (TfosException e) {
        return CompletableFuture.failedFuture(e);
      }
      future.complete(DomainApprovalAction.OK);
    } else {
      service = null;
      // Domain check is applied to a default signup request
      final var getDomainsState = new ContextOpState("getDomains", state);
      getDomainsState.setTenantName(BiosConstants.TENANT_SYSTEM);
      getDomainsState.setStreamName(BiosConstants.STREAM_EMAIL_DOMAINS);
      try {
        final var contextDesc =
            tfosAdmin.getStream(getDomainsState.getTenantName(), getDomainsState.getStreamName());
        getDomainsState.setStreamDesc(contextDesc);
      } catch (TfosException ex) {
        return CompletableFuture.failedFuture(ex);
      }
      final List<List<Object>> domains = new ArrayList<>();
      String currentDomain = primaryEmailDomain.toLowerCase();
      while (currentDomain != null) {
        domains.add(List.of(currentDomain));
        final var elements = currentDomain.split("\\.", 2);
        currentDomain = elements.length == 2 ? elements[1] : null;
      }

      final var getContextFuture = new CompletableFuture<List<Event>>();

      dataEngine.getContextEntriesAsync(
          domains,
          getDomainsState,
          getContextFuture::complete,
          getContextFuture::completeExceptionally);

      getContextFuture
          .thenApplyAsync(
              (entries) -> {
                final var entry = entries.stream().filter((e) -> e != null).findAny();
                if (entry.isEmpty()) {
                  return DomainApprovalAction.OK;
                }

                final String approvalActionSrc =
                    entry.get().get(BiosConstants.ATTR_APPROVAL_ACTION).toString();
                try {
                  final var approvalAction =
                      DomainApprovalAction.valueOf(approvalActionSrc.toUpperCase());
                  logger.info(
                      "Approval action = {} for the domain = {}",
                      approvalAction,
                      primaryEmailDomain);
                  return approvalAction;
                } catch (IllegalArgumentException e) {
                  logger.error("Unknown domain approval action = {}", approvalActionSrc);
                  throw new CompletionException(
                      new TfosException(SignupError.GENERIC_SIGNUP_ERROR));
                }
              },
              state.getExecutor())
          .whenComplete(
              (action, t) -> {
                if (t == null) {
                  future.complete(action);
                } else {
                  future.completeExceptionally(t);
                }
              });
    }

    return future.thenApplyAsync(
        (approvalAction) -> {
          if (approvalAction == DomainApprovalAction.REJECT) {
            poster.execute(() -> sendSignupRejectedEmail(email));
            throw new CompletionException(new TfosException(SignupError.INVALID_EMAIL_DOMAIN));
          }

          final var response = new SignupResponse();
          response.setTenantName(tenantName);
          final var token =
              createApprovalToken(
                  email,
                  tenantName,
                  service,
                  request.getSource(),
                  request.getDomain(),
                  true,
                  request.getAdditionalUsers());

          if (TfosConfig.testSignup()) {
            response.setToken(token);
          } else {
            poster.execute(
                () -> {
                  final var recipient = signupApprovalAdminMailId();
                  if (approvalAction == DomainApprovalAction.OK) {
                    sendSignupApprovalEmail(
                        token, email, recipient, organizationName, tenantName, request);
                    logger.info(
                        "An email is sent for a signup approval request; user={}, recipient={}",
                        email,
                        recipient);
                  } else { // warn
                    sendSignupWarningEmail(email);
                    sendSignupApprovalEmail(
                        token,
                        email,
                        signupApprovalCompetitorAdminMailId(),
                        organizationName,
                        tenantName,
                        request);
                  }
                });
          }
          return response;
        },
        state.getExecutor());
  }

  private String generateCsvConfigEntry(
      String tenantName,
      String dataUser,
      String dataUserPassword,
      String mailRecipient,
      String domain,
      List<String> additionaUsers)
      throws JsonProcessingException {
    final Map<String, Object> tenantConfig = new HashMap<>();
    tenantConfig.put("tenantName", tenantName);
    tenantConfig.put("adminEmailRecipient", mailRecipient);
    tenantConfig.put("dataUser", dataUser);
    tenantConfig.put("dataUserPassword", dataUserPassword);
    tenantConfig.put("numTestMails", 2);
    tenantConfig.put("businessUsers", "");
    if (additionaUsers != null) {
      tenantConfig.put("businessUsers", String.join("::", additionaUsers));
    }
    return String.join(
        ",", List.of(domain, StringEscapeUtils.escapeCsv(mapper.writeValueAsString(tenantConfig))));
  }

  private CompletableFuture<Void> upsertContext(
      SessionToken sessionToken,
      String tenantName,
      String contextName,
      List<String> entries,
      ExecutionState parentState) {
    final var request = new PutContextEntriesRequest();
    request.setContentRepresentation(ContentRepresentation.CSV);
    request.setEntries(entries);

    final var state =
        new ContextWriteOpState<PutContextEntriesRequest, List<Event>>(
            "putContextEntries", parentState.getExecutor());
    state.setInitialParams(
        tenantName, contextName, null, request, RequestPhase.INITIAL, System.currentTimeMillis());

    final FanRouter<PutContextEntriesRequest, Void> fanRouter =
        new HttpFanRouter<PutContextEntriesRequest, Void>(
            POST,
            BiosServicePath.ROOT,
            () -> String.format("/tenants/%s/contexts/%s/entries", tenantName, contextName),
            sessionToken.getToken(),
            clientManager,
            Void.class);

    return BiosModules.getDataServiceHandler()
        .upsertContextEntries(sessionToken, null, state, fanRouter);
  }

  private CompletableFuture<Void> updateContext(
      SessionToken sessionToken,
      String tenantName,
      String contextName,
      List<Object> primaryKey,
      List<AttributeSpec> attributes,
      ExecutionState parentState) {
    final var request = new UpdateContextEntryRequest();
    request.setContentRepresentation(ContentRepresentation.UNTYPED);
    request.setPrimaryKey(primaryKey);
    request.setAttributes(attributes);

    final var state =
        new ContextWriteOpState<UpdateContextEntryRequest, UpdateContextEntrySpec>(
            "updateContextEntries", parentState.getExecutor());
    state.setInitialParams(
        tenantName, contextName, null, request, RequestPhase.INITIAL, System.currentTimeMillis());

    final var fanRouter =
        new HttpFanRouter<UpdateContextEntryRequest, Void>(
            PATCH,
            BiosServicePath.ROOT,
            () -> String.format("/tenants/%s/contexts/%s/entries", tenantName, contextName),
            sessionToken.getToken(),
            clientManager,
            Void.class);

    return BiosModules.getDataServiceHandler()
        .updateContextEntry(sessionToken, null, state, fanRouter);
  }

  public CompletableFuture<String> approveUser(Map<String, String> params, ExecutionState state) {
    final AuditInfo auditInfo;
    try {
      auditInfo =
          auditManager.begin(
              AuditConstants.NA, AuditOperation.APPROVE_USER, mapper.writeValueAsString(params));
    } catch (JsonProcessingException e) {
      return CompletableFuture.failedFuture(
          new ApplicationException("Failed to create audit info", e));
    }

    try {
      final var token = params.get("token");
      if (token == null) {
        return CompletableFuture.failedFuture(new TfosException(SignupError.INVALID_REQUEST));
      }
      final var action = params.get("action");
      if (action == null) {
        return CompletableFuture.failedFuture(new TfosException(SignupError.INVALID_REQUEST));
      }

      final var tokenFuture = new CompletableFuture<UserContext>();
      JwtTokenUtils.parseToken(
          token,
          approvalSecret,
          false,
          state,
          tokenFuture::complete,
          tokenFuture::completeExceptionally);
      return tokenFuture
          .thenComposeAsync(
              (userContext) -> {
                auditInfo.setUserContext(userContext);
                final String email = userContext.getSubject();
                final String orgName = userContext.getTenant();
                if (!validateEmail(email)) {
                  throw new CompletionException(
                      new TfosException(SignupError.INVALID_EMAIL_ADDRESS));
                }

                final var signupApprovalAction =
                    SignupApprovalAction.getSignupApprovalAction(action);

                logger.info(
                    "Signup approval; user={}, tenant={}, action={}",
                    email,
                    orgName,
                    signupApprovalAction);
                if (signupApprovalAction == SignupApprovalAction.APPROVE) {
                  final var source = userContext.getSource();
                  if (null != source && source.equalsIgnoreCase("wordpress")) {
                    ServiceRegistrationRequest request = new ServiceRegistrationRequest();
                    request.setDomain(userContext.getDomain());
                    request.setEmail(email);
                    request.setServiceType(ServiceType.SITE_ANALYTICS);
                    final var sessionToken =
                        JwtTokenUtils.makeTemporaryAppMasterToken(
                            source.toLowerCase(), userContext.getDomain());
                    return registerForService(sessionToken, request, (GenericExecutionState) state)
                        .thenComposeAsync(
                            (serviceRegistrationResponse) -> {
                              final String result = serviceRegistrationResponse.getResult();
                              if (!"TenantCreated".equalsIgnoreCase(result)) {
                                logger.error("Tenant not created. result={}", result);
                                throw new CompletionException(
                                    new RuntimeException(
                                        "Tenant creation failure. Result=" + result));
                              }
                              final String tenantName = serviceRegistrationResponse.getTenantName();
                              final String dataUser = "data+" + tenantName + "@isima.io";
                              final String dataUserPassword =
                                  RandomStringUtils.random(15, passwordChars);
                              logger.info("tenantName={}. dataUser={}", tenantName, dataUser);
                              return organizationRepository
                                  .findByTenantName(tenantName, state)
                                  .thenComposeAsync(
                                      (org) -> {
                                        if (org != null) {
                                          logger.info("*** Creating data user");
                                          final String passwordHash = auth.hash(dataUserPassword);
                                          return userRepository.setupUser(
                                              org.getId(),
                                              TfosUserManager.generateUniqueId(),
                                              "Data ingest/extract user",
                                              dataUser,
                                              passwordHash,
                                              List.of(Permission.INGEST_EXTRACT),
                                              MemberStatus.ACTIVE,
                                              state);
                                        } else {
                                          logger.error("Errors occurred during org creation.");
                                          throw new CompletionException(
                                              new RuntimeException("Org creation failure"));
                                        }
                                      },
                                      state.getExecutor())
                                  .thenComposeAsync(
                                      (user) -> {
                                        if (user != null) {
                                          try {
                                            logger.info("User created. id={}", user.getId());
                                            final String csvEntry;
                                            csvEntry =
                                                generateCsvConfigEntry(
                                                    tenantName,
                                                    dataUser,
                                                    dataUserPassword,
                                                    email,
                                                    userContext.getDomain(),
                                                    userContext.getAdditionalUsers());
                                            final var upsertToken =
                                                JwtTokenUtils.makeTemporarySessionToken(
                                                    "wordpress",
                                                    System.currentTimeMillis(),
                                                    null,
                                                    null);
                                            logger.info(
                                                "Upsert data user credentials to tenants context");
                                            return upsertContext(
                                                upsertToken,
                                                "wordpress",
                                                "tenants",
                                                List.of(csvEntry),
                                                state);
                                          } catch (Throwable t) {
                                            logger.error(
                                                "Error occurred during upsert to tenants context: ",
                                                t);
                                            throw new CompletionException(t);
                                          }
                                        } else {
                                          logger.error("Data user could not be created");
                                          throw new CompletionException(
                                              new TfosException("Data user could not be created"));
                                        }
                                      },
                                      state.getExecutor())
                                  .thenComposeAsync(
                                      (upsertResult) -> {
                                        try {
                                          final long currentTime = System.currentTimeMillis();
                                          final long expirationTime =
                                              currentTime + TfosConfig.signupExpirationTimeMillis();
                                          sendVerificationMailsToAdditionalUsers(
                                              currentTime,
                                              expirationTime,
                                              userContext.getAdditionalUsers(),
                                              userContext);
                                          userContext.setPermissions(List.of(BI_REPORT.getId()));
                                          final String message =
                                              "Signup Approved. Tenant creation in "
                                                  + "progress. Verification mail will be sent to: "
                                                  + email
                                                  + " (ETA: 1 minute).";
                                          return CompletableFuture.completedFuture(message);
                                        } catch (Throwable t) {
                                          logger.error(
                                              "Error occurred while sending mails "
                                                  + "to additional users",
                                              t);
                                          throw new CompletionException(t);
                                        }
                                      },
                                      state.getExecutor());
                            },
                            state.getExecutor())
                        .whenComplete(
                            (r, t) -> {
                              if (t != null) {
                                throw new CompletionException(t);
                              }
                            })
                        .thenApply((message) -> message);
                  } else {
                    String returnValue = "Signup " + signupApprovalAction.name().toLowerCase();
                    final long currentTime = System.currentTimeMillis();
                    final long expirationTime =
                        currentTime + TfosConfig.signupExpirationTimeMillis();
                    final var primaryUser = new UserContext(userContext);
                    primaryUser.setAdditionalUsers(null);
                    final String primaryToken =
                        JwtTokenUtils.createToken(
                            currentTime, expirationTime, primaryUser, verificationSecret);
                    if (!TfosConfig.testSignup()) {
                      try {
                        sendVerifyEmail(email, primaryToken);
                      } catch (Throwable t) {
                        throw new CompletionException(t);
                      }
                      sendVerificationMailsToAdditionalUsers(
                          currentTime,
                          expirationTime,
                          userContext.getAdditionalUsers(),
                          userContext);
                      sendSignupApprovalNotification(email, orgName, signupApprovalAction);
                    } else {
                      returnValue = primaryToken;
                    }
                    return CompletableFuture.completedFuture(returnValue);
                  }
                }
                // Not an APPROVE action !! Should not happen, approval email
                // has only APPROVE action
                throw new CompletionException(new RuntimeException("Invalid Action"));
              },
              state.getExecutor())
          .whenCompleteAsync((r, t) -> commitAuditLog(r, t, auditInfo), state.getExecutor());
    } catch (Throwable t) {
      auditManager.commit(auditInfo, t);
      return CompletableFuture.failedFuture(t);
    }
  }

  private void sendVerificationMailsToAdditionalUsers(
      Long currentTime,
      Long expirationTime,
      List<String> additionalUsers,
      UserContext userContext) {
    if (additionalUsers != null) {
      for (var additionalEmail : additionalUsers) {
        final var additionalUser = new UserContext(userContext);
        additionalUser.setSubject(additionalEmail);
        additionalUser.setPermissions(List.of(BI_REPORT.getId()));
        final String additionalToken =
            JwtTokenUtils.createToken(
                currentTime, expirationTime, additionalUser, verificationSecret);
        sendVerifyEmail(additionalEmail, additionalToken);
      }
    }
  }

  /**
   * Method to verify sign-up token issued to User.
   *
   * @param params An object stringified by JSON
   */
  public CompletableFuture<Map> verifyUser(PasswordSpec params, ExecutionState state) {
    final AuditInfo auditInfo =
        auditManager.begin(AuditConstants.NA, AuditOperation.VERIFY_SIGNUP_TOKEN, "");
    try {
      final String verificationToken = params.getToken();

      final var userFuture = new CompletableFuture<UserContext>();

      JwtTokenUtils.parseToken(
          verificationToken,
          verificationSecret,
          false,
          state,
          userFuture::complete,
          userFuture::completeExceptionally);

      return userFuture
          .thenComposeAsync(
              (userContext) -> {
                logger.info("Verified user: {}", userContext);

                auditInfo.setUser(userContext.getSubject());
                auditInfo.setTenant(userContext.getTenant());
                auditInfo.setUserContext(userContext);
                if (userContext.getAppName() != null) {
                  auditInfo.setRequest(userContext.getAppName());
                }

                final String email = userContext.getSubject();

                String[] tokens = email.split("@");
                if (tokens.length < 2) {
                  return CompletableFuture.failedFuture(
                      new InvalidRequestException("Invalid token"));
                }

                // Name of user parsed from email id
                final String userName = StringUtils.capitalize(tokens[0]);

                // deduce organization name from email domain
                final String parsedDomain = getOrganizationName(tokens[1]);
                final String domain =
                    ObjectUtils.firstNonNull(
                        userContext.getDomain(), userContext.getTenant(), parsedDomain);
                final String tenantName =
                    ObjectUtils.firstNonNull(userContext.getTenant(), parsedDomain);

                logger.info(
                    "Verify signup process; email={}, tenant={}, domain={}",
                    email,
                    tenantName,
                    domain);

                // whether user is invited by colleague or requested for sign-up through bi(OS) UI.
                final boolean isInvite = userContext.getOrgId() != null;

                final AtomicBoolean isOrganizationPresent = new AtomicBoolean();

                final CompletableFuture<Organization> orgFuture;
                if (userContext.getOrgId() == null) {
                  orgFuture = organizationRepository.findByTenantName(tenantName, state);
                } else {
                  orgFuture = organizationRepository.findById(userContext.getOrgId(), state);
                }
                return orgFuture
                    .thenComposeAsync(
                        (organization) -> {
                          if (organization != null) {
                            isOrganizationPresent.set(true);
                            return CompletableFuture.completedFuture(organization);
                          } else {
                            isOrganizationPresent.set(false);
                            return organizationRepository
                                .setupOrg(
                                    TfosUserManager.generateUniqueId(),
                                    tenantName,
                                    tenantName,
                                    state)
                                .thenApplyAsync(
                                    (org) -> {
                                      createTenant(tenantName, domain, null, state);
                                      return org;
                                    },
                                    state.getExecutor());
                          }
                        },
                        state.getExecutor())
                    .thenComposeAsync(
                        (organization) -> {
                          // set the organization which will be used for user creation if not
                          // created
                          userContext.setOrgId(organization.getId());
                          return userRepository.findByEmail(email, state);
                        },
                        state.getExecutor())
                    .thenComposeAsync(
                        (user) -> {
                          if (user != null) {
                            if (user.getStatus() == MemberStatus.ACTIVE) {
                              throw new CompletionException(
                                  new TfosException(SignupError.LINK_EXPIRED));
                            }
                            return CompletableFuture.completedFuture(user);
                          }
                          return createUser(
                              userContext,
                              isOrganizationPresent.get(),
                              isInvite,
                              userName,
                              email,
                              MemberStatus.TO_BE_VERIFIED,
                              state);
                        },
                        state.getExecutor())
                    .thenApply(
                        (user) -> {
                          final Map response = buildVerifyUserResponse(user, tenantName);
                          logger.info(
                              "Signup user verified; user={}, tenant={}, token={}",
                              user,
                              domain,
                              response.get("token"));
                          return response;
                        });
              },
              state.getExecutor())
          .whenCompleteAsync(
              (response, t) -> commitAuditLog(response, t, auditInfo), state.getExecutor());

    } catch (Throwable ex) {
      auditManager.commit(auditInfo, ex);
      return CompletableFuture.failedFuture(ex);
    }
  }

  /** Requests to send email to verify user's email */
  public CompletableFuture<Map> requestEmailVerification(
      Credentials request, GenericExecutionState state) {
    if (request == null || StringUtils.isBlank(request.getEmail())) {
      return CompletableFuture.failedFuture(
          new InvalidRequestException("Parameter 'email' must be specified"));
    }

    final var email = request.getEmail();
    return userRepository
        .findByEmail(email, state)
        .thenComposeAsync(
            (user) -> {
              final var response = new LinkedHashMap<String, String>();
              response.put("email", email);
              if (user == null) {
                response.put("status", "EmailNotFound");
                return CompletableFuture.completedFuture(response);
              }
              if (user.getStatus() != MemberStatus.TO_BE_VERIFIED) {
                response.put("status", "EmailAlreadyVerified");
                return CompletableFuture.completedFuture(response);
              }
              final var orgId = user.getOrgId();
              return organizationRepository
                  .findById(orgId, state)
                  .thenApplyAsync(
                      (org) -> {
                        if (org == null) {
                          throw new CompletionException(
                              new ApplicationException(
                                  String.format(
                                      "User found in the table but associated organization is missing; user=%s",
                                      user)));
                        }
                        long currentTime = System.currentTimeMillis();
                        long expirationTime = currentTime + TfosConfig.signupExpirationTimeMillis();
                        final var userContext = new UserContext();
                        userContext.setSubject(user.getEmail());
                        userContext.setTenant(org.getTenant());
                        userContext.setUserId(user.getId());
                        userContext.setOrgId(org.getId());
                        userContext.setAdditionalUsers(null);
                        final String primaryToken =
                            JwtTokenUtils.createToken(
                                currentTime, expirationTime, userContext, verificationSecret);
                        try {
                          logger.info(
                              "Sending verification email; email={}, tenant={}",
                              email,
                              org.getTenant());
                          sendVerifyEmail(email, primaryToken);
                        } catch (Throwable t) {
                          throw new CompletionException(t);
                        }
                        response.put("status", "VerificationEmailSent");
                        return response;
                      },
                      state.getExecutor());
            },
            state.getExecutor());
  }

  /**
   * Method to complete the sign-up process.
   *
   * @param params An object stringified by JSON
   */
  public CompletableFuture<Map> completeUserSignup(PasswordSpec params, ExecutionState state) {
    Objects.requireNonNull(params);
    Objects.requireNonNull(state);

    final AuditInfo auditInfo =
        auditManager.begin(AuditConstants.NA, AuditOperation.COMPLETE_SIGNUP, "");

    try {
      String finalizationToken = params.getToken();
      String password = params.getPassword();
      String userName = params.getUsername();
      // validate only password here - token will be validated in parseToken, username is always
      // non-null
      if (password == null || password.isEmpty()) {
        return CompletableFuture.failedFuture(new TfosException(SignupError.INVALID_REQUEST));
      }

      final var userFuture = new CompletableFuture<UserContext>();

      JwtTokenUtils.parseToken(
          finalizationToken,
          finalizationSecret,
          false,
          state,
          userFuture::complete,
          userFuture::completeExceptionally);

      return userFuture
          .thenComposeAsync(
              (userContext) -> {
                auditInfo.setUserContext(userContext);
                auditInfo.setTenant(userContext.getTenant());
                if (userContext.getAppName() != null) {
                  auditInfo.setRequest(userContext.getAppName());
                }
                return userRepository
                    .findById(userContext.getOrgId(), userContext.getUserId(), true, state)
                    .thenCompose(
                        (user) -> {
                          // update user password and enable the user
                          Date date = new Date();
                          user.setUpdatedAt(date);
                          user.setStatus(MemberStatus.ACTIVE);
                          if (!userName.isBlank()) {
                            user.setName(userName);
                          }
                          user.setPassword(auth.hash(password));
                          return userRepository.save(user, state);
                        })
                    .thenComposeAsync(
                        (user) -> {
                          final String mailToken =
                              JwtTokenUtils.makeTemporarySessionToken(
                                      userContext.getTenant(), System.currentTimeMillis())
                                  .getToken();
                          sendRegistrationCompleteEmail(user.getEmail(), mailToken);
                          return CompletableFuture.completedFuture(user);
                        },
                        state.getExecutor())
                    .thenComposeAsync(
                        (user) -> {
                          Map responseContent = new HashMap<String, String>();
                          responseContent.put("email", user.getEmail());
                          try {
                            final var jhAdmin = BiosModules.getJupyterHubAdmin();
                            try {
                              jhAdmin.createUser(user.getEmail());
                            } catch (TfosException e) {
                              if (e.getStatus() != Response.Status.CONFLICT) {
                                // rethrow
                                throw e;
                              }
                              logger.warn(
                                  "JupyterHub user exists already, skipping to create;"
                                      + " user={}, tenant={}, error={}",
                                  user.getEmail(),
                                  userContext.getTenant(),
                                  e.toString());
                              return CompletableFuture.completedFuture(responseContent);
                            }
                            String hubKey = jhAdmin.createTokenForUser(user.getEmail());
                            String encodedEmail = jhAdmin.encodeEmail(user.getEmail());
                            user.setDevInstance(
                                new StringBuilder().append("hub_").append(encodedEmail).toString());
                            user.setSshUser(user.getName());
                            user.setSshIp(TfosConfig.getJupyterHubUrl());
                            user.setSshKey(hubKey);
                            user.setSshPort(0);
                            user.setCloudId(0);
                            return userRepository
                                .update(user, state)
                                .thenApply(
                                    (updatedUser) -> {
                                      logger.info(
                                          "Signup completed; user={}, tenant={}",
                                          user.getEmail(),
                                          userContext.getTenant());
                                      return responseContent;
                                    });
                          } catch (TfosException e) {
                            logger.error("Signing up to JupyterHub failed", e);
                            logger.info(
                                "Signup completed without JupyterHub registration; user={}, tenant={}",
                                user.getEmail(),
                                userContext.getTenant());
                            return CompletableFuture.completedFuture(responseContent);
                          }
                        },
                        state.getExecutor());
              },
              state.getExecutor())
          .whenCompleteAsync(
              (response, t) -> commitAuditLog(response, t, auditInfo), state.getExecutor());
    } catch (Throwable t) {
      auditManager.commit(auditInfo, t);
      return CompletableFuture.failedFuture(t);
    }
  }

  /**
   * Method to invite a user in a tenant.
   *
   * @param sessionToken The session token
   * @param request An InviteRequest object as request body
   */
  public CompletableFuture<Void> inviteUser(
      SessionToken sessionToken, InviteRequest request, ExecutionState state) {
    final AuditInfo auditInfo;
    try {
      auditInfo =
          auditManager.begin(
              AuditConstants.NA, AuditOperation.INVITE_USER, mapper.writeValueAsString(request));
    } catch (JsonProcessingException e) {
      return CompletableFuture.failedFuture(
          new ApplicationException("Failed to create audit info", e));
    }

    logger.info("Invite a user in a tenant");
    UserContext requesterUserContext;
    try {
      requesterUserContext = validateTokenCore(sessionToken, "", Arrays.asList(ADMIN), state);
      auditInfo.setUserContext(requesterUserContext);
      auditInfo.setTenant(requesterUserContext.getTenant());
    } catch (UserAccessControlException e) {
      auditManager.commit(auditInfo, e);
      return CompletableFuture.failedFuture(e);
    }
    String email = request.getEmail();
    auditInfo.setUser(email);
    if (!validateEmail(email)) {
      final var e = new TfosException(SignupError.INVALID_EMAIL_ADDRESS);
      auditManager.commit(auditInfo, e);
      return CompletableFuture.failedFuture(e);
    }
    List<Role> roles = request.getRoles();
    List<Integer> permissions = new ArrayList<>();
    for (Role role : roles) {
      permissions.add(Permission.forBios(role).getId());
    }
    UserContext inviteeUserContext = new UserContext();
    inviteeUserContext.setSubject(email);
    inviteeUserContext.setPermissions(permissions);
    inviteeUserContext.setOrgId(requesterUserContext.getOrgId());
    inviteeUserContext.setTenant(requesterUserContext.getTenant());
    return CompletableFuture.runAsync(
            () -> {
              sendInviteEmail(inviteeUserContext);
              logger.info("Invite user email sent; recipient={}, roles={}", email, roles);
            },
            state.getExecutor())
        .whenCompleteAsync((r, t) -> commitAuditLog(r, t, auditInfo), state.getExecutor());
  }

  private Service verifyServiceRegistrationRequest(SignupRequest request) throws TfosException {
    if (request.getAdditionalUsers() != null) {
      for (int i = 0; i < request.getAdditionalUsers().size(); ++i) {
        final var user = request.getAdditionalUsers().get(i);
        if (user == null) {
          throw new InvalidRequestException("additionalUsers[%d] is null", i);
        }
        if (!validateEmail(user)) {
          throw new TfosException(
              SignupError.INVALID_EMAIL_ADDRESS, String.format("additionalUsers[%d]=%s", i, user));
        }
      }
    }
    final Service service;
    try {
      service = Service.forValue(request.getService());
    } catch (IllegalArgumentException e) {
      throw new InvalidRequestException("Unknown service: %s", request.getService());
    }
    switch (service) {
      case BOT_DETECTION:
      case ANALYTICS:
        if (request.getDomain() == null || request.getDomain().isBlank()) {
          throw new InvalidRequestException("Domain must be set");
        }
        if (request.getSource() == null) {
          throw new InvalidRequestException("Source must be set");
        }
    }
    return service;
  }

  // Utilities //////////////////////////////////////////////////////////

  private String getOrganizationName(String domain) {
    final String orgName;
    final var tokens = domain.split("[.]");
    if (tokens.length < 2) {
      orgName = domain;
    } else {
      orgName = tokens[0];
    }
    return orgName;
  }

  private CompletableFuture<User> createUser(
      UserContext userContext,
      boolean isOrganizationPresent,
      boolean isInvite,
      String name,
      String email,
      MemberStatus userStatus,
      ExecutionState state) {
    // User who signed up through bi(OS) UI, will have "AdminInternal" permission.
    // User who are invited by colleague through UI, will have list of permissions
    // User who are invited by colleague through (selfserve/v1/auth/invite),
    // will have "Business user", "Ingest" and "Extract" permission
    final List<Permission> permissions;
    if (userContext.getPermissions() != null && !userContext.getPermissions().isEmpty()) {
      permissions = userContext.getPermissions();
    } else if (!isOrganizationPresent) {
      permissions = tenantAdminRole;
    } else if (isInvite) {
      permissions = mlEngineerRole;
    } else {
      permissions = tenantAdminRole;
    }

    // create user (with disabled state).
    return userRepository.setupUser(
        userContext.getOrgId(),
        TfosUserManager.generateUniqueId(),
        name,
        email,
        null,
        permissions,
        userStatus,
        state);
  }

  private Map<String, String> buildVerifyUserResponse(User user, String tenantName) {
    long currentTime = System.currentTimeMillis();
    long expirationTime = currentTime + TfosConfig.signupExpirationTimeMillis();
    UserContext context = new UserContext();
    context.setSubject(auth.hash(user.getEmail()));
    context.setTenant(tenantName);
    context.setUserId(user.getId());
    context.setOrgId(user.getOrgId());

    String finalizationToken =
        JwtTokenUtils.createToken(currentTime, expirationTime, context, finalizationSecret);

    return Map.of("token", finalizationToken);
  }

  private UserContext parseToken(String token, boolean isCookie) throws UserAccessControlException {
    final var sessionToken = new SessionToken(token, isCookie);
    return auth.parseToken(sessionToken);
  }

  private <T> void commitAuditLog(T response, Throwable t, AuditInfo auditInfo) {
    if (t != null) {
      final var cause = t instanceof CompletionException ? t.getCause() : t;
      auditManager.commit(auditInfo, cause);
    }
    try {
      String result = "";
      if (response != null) {
        result = mapper.writeValueAsString(response);
      }
      auditManager.commit(auditInfo, Response.Status.OK.name(), result);
    } catch (JsonProcessingException e) {
      throw new CompletionException(e);
    }
  }

  private CompletableFuture<Void> createTenant(
      final String tenantName, String domain, String appMaster, ExecutionState state) {
    if (adminServiceHandler == null) {
      return CompletableFuture.completedFuture(null);
    }

    final List<Integer> permissionIds =
        AllowedRoles.SYSADMIN.stream().map(Permission::getId).collect(Collectors.toList());

    final UserContext userContext =
        new UserContext(
            TfosConfig.DEFAULT_SUPERADMIN_ID,
            TfosConfig.DEFAULT_ORG_ID,
            TfosConfig.DEFAULT_SUPERADMIN_USERNAME,
            BiosConstants.TENANT_SYSTEM,
            permissionIds,
            "sign_up_completion",
            AppType.INTERNAL);

    long currentTime = System.currentTimeMillis();
    final long expirationMillis = 120000; // finish within two minutes
    long expirationTime = currentTime + expirationMillis;

    final String sessionToken = auth.createToken(currentTime, expirationTime, userContext);
    final var fanRouter =
        new HttpFanRouter<TenantConfig, Void>(
            "POST", "", () -> PATH_TENANTS, sessionToken, clientManager, Void.class);

    final var tenantConfig = new TenantConfig(tenantName);
    tenantConfig.setDomains(domain != null ? List.of(domain) : null);
    tenantConfig.setAppMaster(appMaster);
    final var registerApps = false;
    final var createTenantState = new GenericExecutionState("Signup/CreateTenant", state);
    createTenantState.addHistory("(createTenant{");
    return organizationRepository
        .setupOrg(
            TfosUserManager.generateUniqueId(),
            domain != null ? domain : tenantName,
            tenantName,
            state)
        .thenComposeAsync(
            (none) ->
                adminServiceHandler.createTenant(
                    new SessionToken(sessionToken, false),
                    tenantConfig,
                    RequestPhase.INITIAL,
                    currentTime,
                    Optional.of(fanRouter),
                    registerApps,
                    createTenantState),
            state.getExecutor())
        .whenCompleteAsync(
            (result, t) -> {
              if (t != null) {
                final var cause = t.getCause() != null ? t.getCause() : t;
                if (!(cause instanceof TenantAlreadyExistsException)) {
                  createTenantState.markError();
                  throw new CompletionException("Tenant creation failed", cause);
                }
              }
              createTenantState.markDone();
            },
            state.getExecutor());
  }

  private CompletableFuture<ContextConfig> createContext(
      final String tenantName,
      ContextConfig contextConfig,
      String sessionToken,
      ExecutionState state) {
    if (adminServiceHandler == null) {
      return CompletableFuture.completedFuture(null);
    }

    final long currentTime = System.currentTimeMillis();
    final var signalFanRouter =
        new HttpFanRouter<SignalConfig, SignalConfig>(
            "POST",
            "",
            () -> PATH_SIGNALS.replace("{tenantName}", tenantName),
            sessionToken,
            clientManager,
            SignalConfig.class);

    final var contextFanRouter =
        new HttpFanRouter<ContextConfig, ContextConfig>(
            "POST",
            "",
            () -> PATH_CONTEXTS.replace("{tenantName}", tenantName),
            sessionToken,
            clientManager,
            ContextConfig.class);
    contextFanRouter.addParam(
        Keywords.X_BIOS_AUDIT_ENABLED,
        Boolean.toString(contextConfig.getAuditEnabled() == Boolean.TRUE));

    final var createContextState = new GenericExecutionState("createContext", state);
    createContextState.addHistory("(createContext{");
    final var auditSignalName =
        io.isima.bios.utils.StringUtils.prefixToCamelCase(
            CONTEXT_AUDIT_SIGNAL_PREFIX, contextConfig.getName());
    return adminServiceHandler
        .createContext(
            new SessionToken(sessionToken, false),
            tenantName,
            contextConfig,
            RequestPhase.INITIAL,
            currentTime,
            Optional.of(contextFanRouter),
            auditSignalName,
            Optional.of(signalFanRouter),
            createContextState)
        .whenComplete(
            (result, t) -> {
              if (t != null) {
                final var cause = t.getCause() != null ? t.getCause() : t;
                if (!(cause instanceof TenantAlreadyExistsException)) {
                  createContextState.markError();
                }
              }
              createContextState.markDone();
            });
  }

  private CompletableFuture<SignalConfig> createSignal(
      final String tenantName,
      SignalConfig signalConfig,
      String sessionToken,
      ExecutionState state) {
    if (adminServiceHandler == null) {
      return CompletableFuture.completedFuture(null);
    }

    final long currentTime = System.currentTimeMillis();

    final var signalFanRouter =
        new HttpFanRouter<SignalConfig, SignalConfig>(
            "POST",
            "",
            () -> PATH_SIGNALS.replace("{tenantName}", tenantName),
            sessionToken,
            clientManager,
            SignalConfig.class);

    final var createContextState = new GenericExecutionState("createSignal", state);
    createContextState.addHistory("(createSignal{");
    return adminServiceHandler
        .createSignal(
            new SessionToken(sessionToken, false),
            tenantName,
            signalConfig,
            RequestPhase.INITIAL,
            currentTime,
            Optional.of(signalFanRouter),
            createContextState)
        .whenComplete(
            (result, t) -> {
              if (t != null) {
                final var cause = t.getCause() != null ? t.getCause() : t;
                if (!(cause instanceof TenantAlreadyExistsException)) {
                  createContextState.markError();
                }
              }
              createContextState.markDone();
            });
  }

  // email sending methods /////////////////////////////////////////////

  private void sendSignupApprovalNotification(
      String mailId, String orgName, SignupApprovalAction action) {
    final List<String> bccList = TfosConfig.signupApprovalMailBccList();
    final String mailContent =
        String.format(TfosConfig.signupApprovalNotificationMailContent(), action.getSignupAction());
    final String body = makeMailBody(mailContent);
    final Email emailObj =
        Email.builder()
            .subject(getSignupApprovalMailSubject(mailId, orgName))
            .textHtml(body)
            .to(signupApprovalAdminMailId())
            .bcc(bccList)
            .from(TfosConfig.mailServiceFromAddress())
            .build();
    mailClient.sendMail(emailObj);
  }

  private String getSignupApprovalMailSubject(String mailId, String orgName) {
    if (orgName == null) {
      return String.format(signupApprovalMailSubjectTemplateTenant(), mailId);
    } else {
      return String.format(signupApprovalMailSubjectTemplateUser(), mailId);
    }
  }

  protected String sendSignupAttemptNotificationToAdministrator(String mailId, String tenantName) {
    logger.info("signup attempt email send for the user = {}", mailId);
    final List<String> bccList = TfosConfig.signupApprovalMailBccList();
    String mailContent =
        String.format(
            TfosConfig.signupAttemptNotificationMailContent(),
            mailId,
            tenantName,
            biosDomainName());
    final String body = makeMailBody(mailContent);
    final Email emailObj =
        Email.builder()
            .subject(String.format(signupAttemptNotificationSubjectTemplate(), mailId))
            .textHtml(body)
            .to(signupApprovalAdminMailId())
            .bcc(bccList)
            .from(TfosConfig.mailServiceFromAddress())
            .build();
    mailClient.sendMail(emailObj);
    return mailContent;
  }

  private String sendSignupApprovalEmail(
      String token,
      String userEmail,
      String toEmail,
      String organizationName,
      String tenantName,
      SignupRequest request) {
    final var approvalUrl =
        String.format(TfosConfig.adminApprovalBaseUrlTemplate(), biosDomainName());
    String mailContent =
        makeApprovalMailContent(
            token, approvalUrl, userEmail, organizationName, tenantName, request);
    final String body = makeMailBody(mailContent);
    final List<String> bccList = TfosConfig.signupApprovalMailBccList();
    final Email emailObj =
        Email.builder()
            .subject(getSignupApprovalMailSubject(userEmail, tenantName))
            .textHtml(body)
            .to(toEmail)
            .bcc(bccList)
            .from(TfosConfig.mailServiceFromAddress())
            .build();
    mailClient.sendMail(emailObj);
    return mailContent;
  }

  private String makeApprovalMailContent(
      String token,
      String signupApprovalBaseUrl,
      String userEmail,
      String organizationName,
      String tenantName,
      SignupRequest request) {
    final var template = TfosConfig.signupApprovalMailContent();
    final var delimiter = "<br />";
    final var params = new StringBuilder();
    params.append(delimiter).append("email = ").append(userEmail);
    if (tenantName != null) {
      params.append(delimiter).append("tenant = ").append(tenantName);
    }
    if (organizationName != null) {
      params.append(delimiter).append("organization = ").append(organizationName);
    }
    if (request.getService() != null) {
      params.append(delimiter).append("service = ").append(request.getService());
    }
    if (request.getSource() != null) {
      params.append(delimiter).append("source = ").append(request.getSource());
    }
    if (request.getAdditionalUsers() != null) {
      params.append(delimiter).append("additionalUsers = ").append(request.getAdditionalUsers());
    }
    return String.format(template, biosDomainName(), params, signupApprovalBaseUrl, token);
  }

  private String createApprovalToken(
      String email,
      String tenantName,
      Service service,
      String source,
      String serviceDomain,
      boolean isPrimaryUser,
      List<String> additionalUsers) {
    long currentTime = System.currentTimeMillis();
    long expirationTime = currentTime + TfosConfig.signupApprovalExpirationTimeMillis();

    final var userContext = new UserContext();
    userContext.setSubject(email);
    userContext.setTenant(tenantName);
    if (service != null) {
      userContext.setAppName(service.name());
    }
    userContext.setDomain(serviceDomain);
    userContext.setSource(source);
    userContext.setAdditionalUsers(additionalUsers);
    if (isPrimaryUser) {
      userContext.setPermissions(List.of(ADMIN.getId(), BI_REPORT.getId()));
    } else {
      userContext.setPermissions(List.of(BI_REPORT.getId()));
    }
    return JwtTokenUtils.createToken(currentTime, expirationTime, userContext, approvalSecret);
  }

  String sendVerifyEmail(String userEmail, String token) {
    final String verifyUrlFormat = TfosConfig.signupVerifyBaseUrl();
    final String verifyUrl = String.format(verifyUrlFormat, token);
    final String mailContent = String.format(TfosConfig.signupMailContent(), verifyUrl);
    final String body = makeMailBody(mailContent);
    final List<String> bccList = TfosConfig.signupMailBccList();
    final Email emailObj =
        Email.builder()
            .subject("Welcome to bi(OS) - Verification Pending")
            .textHtml(body)
            .to(userEmail)
            .bcc(bccList)
            .from(TfosConfig.mailServiceFromAddress())
            .build();

    mailClient.sendMail(emailObj);
    return mailContent;
  }

  String sendRegistrationCompleteEmail(String userEmail, String token) {
    final String mailContent =
        String.format(
            TfosConfig.registrationCompleteMailContent(), "https://" + TfosConfig.biosDomainName());
    final List<String> bccList = TfosConfig.signupMailBccList();
    final Email emailObj =
        Email.builder()
            .subject("Welcome to bi(OS) - Verification Completed")
            .textHtml(makeMailBody(mailContent))
            .to(userEmail)
            .bcc(bccList)
            .from(TfosConfig.mailServiceFromAddress())
            .build();

    mailClient.sendMail(emailObj);
    return mailContent;
  }

  String sendInviteEmail(UserContext userContext) {
    long currentTime = System.currentTimeMillis();
    long expirationTime = currentTime + TfosConfig.signupExpirationTimeMillis();
    final String token =
        JwtTokenUtils.createToken(currentTime, expirationTime, userContext, verificationSecret);
    final String verifyUrlFormat = TfosConfig.invitationVerifyBaseUrl();
    final String verifyUrl = String.format(verifyUrlFormat, token);
    final String mailContent = String.format(TfosConfig.signupMailContent(), verifyUrl);
    final String body = makeMailBody(mailContent);
    final List<String> bccList = TfosConfig.signupMailBccList();
    final Email emailObj =
        Email.builder()
            .subject("Welcome to bi(OS)")
            .textHtml(body)
            .to(userContext.getSubject())
            .bcc(bccList)
            .from(TfosConfig.mailServiceFromAddress())
            .build();

    mailClient.sendMail(emailObj);
    return mailContent;
  }

  String sendSignupWarningEmail(String mailId) {
    final List<String> bccList = TfosConfig.signupApprovalMailBccList();
    final Email emailObj =
        Email.builder()
            .subject(String.format(signupWarningAdminSubjectTemplate(), mailId))
            .textHtml(String.format(signupWarningAdminBodyTemplate(), mailId))
            .to(signupApprovalAdminMailId())
            .bcc(bccList)
            .from(TfosConfig.mailServiceFromAddress())
            .build();
    mailClient.sendMail(emailObj);
    return String.format(signupWarningAdminBodyTemplate(), mailId);
  }

  String sendSignupRejectedEmail(String mailId) {
    final List<String> bccList = TfosConfig.signupApprovalMailBccList();
    final Email emailObj =
        Email.builder()
            .subject(String.format(signupRejectedAdminSubjectTemplate(), mailId))
            .textHtml(String.format(signupRejectedAdminBodyTemplate(), mailId))
            .to(signupApprovalAdminMailId())
            .bcc(bccList)
            .from(TfosConfig.mailServiceFromAddress())
            .build();
    mailClient.sendMail(emailObj);
    return String.format(signupRejectedAdminBodyTemplate(), mailId);
  }

  String sendAlreadySignedUpEmail(String mailId, String token, String userName) {
    logger.info("already signup email send for the user = {}", mailId);
    final List<String> bccList = TfosConfig.signupApprovalMailBccList();
    final String passwordResetUrlFormat = TfosConfig.forgotPasswordBaseUrl();
    final String passwordResetUrl = String.format(passwordResetUrlFormat, token);
    final String mailContent =
        String.format(TfosConfig.alreadySignupMailContent(), userName, passwordResetUrl);
    final String body = makeMailBody(mailContent);
    final Email emailObj =
        Email.builder()
            .subject("Greeting from bi(OS)")
            .textHtml(body)
            .to(mailId)
            .bcc(bccList)
            .from(TfosConfig.mailServiceFromAddress())
            .build();
    mailClient.sendMail(emailObj);
    return mailContent;
  }

  String sendPasswordResetEmail(String mailId, String token, String userName) {
    final String passwordResetUrlFormat = TfosConfig.forgotPasswordBaseUrl();
    final String passwordResetUrl = String.format(passwordResetUrlFormat, token);
    final String mailContent =
        String.format(TfosConfig.forgotPasswordMailContent(), userName, passwordResetUrl);
    final Email emailObj =
        Email.builder()
            .subject("Reset Your Password for bi(OS)")
            .textHtml(mailContent)
            .to(mailId)
            .from(TfosConfig.mailServiceFromAddress())
            .build();
    mailClient.sendMail(emailObj);
    return mailContent;
  }

  private String makeMailBody(String mailContent) {
    final String lineSeparator = System.getProperty("line.separator");
    if (StringUtils.isNotBlank(lineSeparator)) {
      return String.join(lineSeparator, StringUtils.replace(mailContent, "\n", lineSeparator));
    }
    return mailContent;
  }

  public CompletableFuture<ServiceRegistrationResponse> registerForService(
      SessionToken sessionToken, ServiceRegistrationRequest request, GenericExecutionState state) {
    final long requestReceiveTime = System.currentTimeMillis();
    final UserContext delegateUserContext;
    try {
      delegateUserContext =
          validateTokenCore(
              sessionToken, "", List.of(Permission.APP_MASTER, Permission.SUPERADMIN), state);
    } catch (UserAccessControlException e) {
      return CompletableFuture.failedFuture(e);
    }
    final var serviceType = request.getServiceType();
    final var domain = request.getDomain().trim();
    if (domain.isEmpty()) {
      return CompletableFuture.failedFuture(
          new InvalidRequestException("Domain must be specified"));
    }
    final var email = request.getEmail().trim();
    if (email.isEmpty()) {
      return CompletableFuture.failedFuture(new InvalidRequestException("Email must be specified"));
    }

    final var lock = new AtomicReference<WorkerLock.LockEntity>();
    // final var organization = new AtomicReference<Organization>();
    final var user = new AtomicReference<User>();
    final var emailIndex = new AtomicInteger(0);
    final var newEmail = new AtomicReference<>(email);

    final var response = new ServiceRegistrationResponse();

    final var lockTarget = "__createTenant." + domain;

    return workerLock
        .lockAsync(lockTarget, ExecutorManager.getSidelineExecutor())
        .thenComposeAsync(
            (acquiredLock) -> {
              if (acquiredLock == null) {
                response.setResult("RequestConflicted");
                return CompletableFuture.completedFuture(response);
              }
              lock.set(acquiredLock);
              return organizationRepository
                  .findByDomain(domain, state)
                  .thenComposeAsync(
                      (organization) -> {
                        return AsyncTrampoline.asyncDoWhile(
                                (em) ->
                                    userRepository
                                        .findByEmail(em, state)
                                        .thenApply(
                                            (usr) -> {
                                              user.set(usr);
                                              if (usr == null
                                                  || (organization != null
                                                      && usr.getOrgId()
                                                          .equals(organization.getId()))) {
                                                return null;
                                              }
                                              final var emElements = email.split("@");
                                              final var nextEmail =
                                                  String.format(
                                                      "%s+shopify%d@%s",
                                                      emElements[0],
                                                      emailIndex.incrementAndGet(),
                                                      emElements[1]);
                                              newEmail.set(nextEmail);
                                              return nextEmail;
                                            }),
                                email,
                                (em) -> em != null)
                            .thenApply((none) -> organization);
                      },
                      state.getExecutor())
                  .thenComposeAsync(
                      (organization) -> {
                        if (organization == null) {
                          final var tenantName = makeTenantName(domain);
                          response.setResult("TenantCreated");
                          response.setTenantName(tenantName);
                          return onRegistrationRequested(
                                  domain,
                                  tenantName,
                                  newEmail.get(),
                                  serviceType,
                                  delegateUserContext,
                                  state)
                              .thenComposeAsync(
                                  (x) ->
                                      createTenant(
                                          tenantName,
                                          domain,
                                          delegateUserContext.getTenant(),
                                          state),
                                  state.getExecutor())
                              .thenComposeAsync(
                                  (x) -> setupSchema(tenantName, serviceType, state),
                                  state.getExecutor())
                              .thenComposeAsync(
                                  (x) -> organizationRepository.findByTenantName(tenantName, state),
                                  state.getExecutor())
                              .thenComposeAsync(
                                  (org) -> {
                                    return onAppTenantSetUp(
                                            domain,
                                            tenantName,
                                            newEmail.get(),
                                            serviceType,
                                            delegateUserContext,
                                            state)
                                        .thenApply((x) -> org);
                                  },
                                  state.getExecutor());
                        }
                        response.setResult("TenantAlreadyExists");
                        response.setTenantName(organization.getTenant());
                        return CompletableFuture.completedFuture(organization);
                      },
                      state.getExecutor())
                  .thenComposeAsync(
                      (org) -> {
                        if (user.get() == null && "TenantCreated".equals(response.getResult())) {
                          final var userContext = new UserContext();
                          userContext.setSubject(newEmail.get());
                          userContext.setTenant(org.getTenant());
                          userContext.setOrgId(org.getId());
                          userContext.setPermissions(
                              List.of(Permission.forBios(Role.REPORT).getId()));
                          return createUser(
                                  userContext,
                                  true,
                                  false,
                                  "",
                                  newEmail.get(),
                                  MemberStatus.TO_BE_VERIFIED,
                                  state)
                              .thenComposeAsync(
                                  (createdUser) -> {
                                    userContext.setUserId(createdUser.getId());
                                    return setupReports(
                                        userContext.getTenant(), serviceType, userContext, state);
                                  },
                                  state.getExecutor())
                              .thenComposeAsync(
                                  (none) -> {
                                    final var verificationRequest =
                                        new Credentials(newEmail.get(), null);
                                    return requestEmailVerification(verificationRequest, state);
                                  },
                                  state.getExecutor());
                        }
                        return CompletableFuture.completedFuture(null);
                      },
                      state.getExecutor())
                  .thenApply(
                      (none) -> {
                        response.setInitialUserEmail(newEmail.get());
                        return response;
                      });
            },
            state.getExecutor())
        .whenComplete(
            (r, t) -> {
              if (lock.get() != null) {
                lock.get().releaseAsync(ExecutorManager.getSidelineExecutor());
              }
              auditRegistration(
                  serviceType,
                  delegateUserContext,
                  domain,
                  request,
                  requestReceiveTime,
                  r,
                  t,
                  state);
            })
        .toCompletableFuture();
  }

  private CompletionStage<Void> onRegistrationRequested(
      String domain,
      String tenantName,
      String email,
      ServiceType serviceType,
      UserContext delegateUserContext,
      GenericExecutionState state) {
    if (serviceType != ServiceType.STORE_ENHANCEMENT) {
      return CompletableFuture.completedStage(null);
    }

    final var upsertToken =
        JwtTokenUtils.makeTemporarySessionToken(
            delegateUserContext.getTenant(),
            System.currentTimeMillis(),
            delegateUserContext.getSubject(),
            null);

    final String now = Long.valueOf(Instant.now().toEpochMilli()).toString();
    final var attributes = List.of(domain, tenantName, email, "RegistrationRequested", now, now);
    final var entries = List.of(String.join(",", attributes));

    return upsertContext(upsertToken, delegateUserContext.getTenant(), "merchants", entries, state);
  }

  private CompletionStage<Void> onAppTenantSetUp(
      String domain,
      String tenantName,
      String email,
      ServiceType serviceType,
      UserContext delegateUserContext,
      GenericExecutionState state) {
    if (serviceType != ServiceType.STORE_ENHANCEMENT) {
      return CompletableFuture.completedStage(null);
    }

    final var updateToken =
        JwtTokenUtils.makeTemporarySessionToken(
            delegateUserContext.getTenant(),
            System.currentTimeMillis(),
            delegateUserContext.getSubject(),
            null);

    final String now = Long.valueOf(Instant.now().toEpochMilli()).toString();

    final var attributes =
        List.of(new AttributeSpec("status", "Active"), new AttributeSpec("updatedAt", now));

    return updateContext(
        updateToken,
        delegateUserContext.getTenant(),
        "merchants",
        List.of(domain),
        attributes,
        state);
  }

  private void auditRegistration(
      ServiceType serviceType,
      UserContext delegateUserContext,
      String domain,
      ServiceRegistrationRequest request,
      long eventTime,
      ServiceRegistrationResponse response,
      Throwable t,
      GenericExecutionState state) {
    if (serviceType != ServiceType.STORE_ENHANCEMENT) {
      return;
    }
    final var objectMapper = BiosObjectMapperProvider.get();
    String payload = "MISSING";
    try {
      payload = objectMapper.writeValueAsString(request);
    } catch (JsonProcessingException e) {
      // ignore, shouldn't happen
    }
    String responseText = "MISSING";
    String status = "OK";
    try {
      if (response != null) {
        responseText = objectMapper.writeValueAsString(response);
      } else if (t != null) {
        responseText = objectMapper.writeValueAsString(t);
        status = "ERROR";
      }
    } catch (JsonProcessingException e) {
      // ignore
    }
    final var event = new EventJson();
    event.setEventId(Generators.timeBasedGenerator().generate());
    event.setIngestTimestamp(new Date(Utils.uuidV1TimestampInMillis(event.getEventId())));
    event.set("eventType", "registration");
    event.set("domain", domain);
    event.set("webhookId", "MISSING");
    event.set("eventTimestamp", eventTime);
    event.set("payload", payload);
    event.set("slackNotification", "MISSING");
    event.set("status", status);
    event.set("response", responseText);
    final var signalDesc =
        tfosAdmin.getStreamOrNull(delegateUserContext.getTenant(), "shopifyAppAudit");
    if (signalDesc == null) {
      return;
    }
    dataEngine.insertEventIgnoreErrors(signalDesc, event, state);
  }

  private void createAuditPageViewSignal(
      TenantConfig tenantConfig,
      String tenantName,
      String sessionToken,
      GenericExecutionState state) {
    SignalConfig auditPageViewConfig = null;
    for (SignalConfig config : tenantConfig.getSignals()) {
      if (config.getName().equalsIgnoreCase("auditPageViews_pageUriBySessionId")) {
        auditPageViewConfig = config;
        break;
      }
    }
    if (null != auditPageViewConfig) {
      try {
        createSignal(tenantName, auditPageViewConfig, sessionToken, state).get();
        tenantConfig.getSignals().remove(auditPageViewConfig);
      } catch (InterruptedException e) {
        logger.error("Interrupted while creating auditPageView FAC", e);
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        logger.error("Error occurred while creating auditPageView FAC", e);
      }
    }
  }

  private CompletionStage<Integer> setupSchema(
      String tenantName, ServiceType serviceType, GenericExecutionState state) {
    final TenantConfig tenantConfig;
    final TenantData initialData;
    switch (serviceType) {
      case STORE_ENHANCEMENT:
        tenantConfig = getStoreEnhancementSchema();
        initialData = getStoreEnhancementInitialData();
        break;
      case SITE_ANALYTICS:
        tenantConfig = getSiteAnalyticsSchema();
        initialData = TenantData.EMPTY;
        break;
      default:
        throw new UnsupportedOperationException(serviceType.toString());
    }

    final List<Integer> permissionIds =
        AllowedRoles.TENANT_WRITE.stream().map(Permission::getId).collect(Collectors.toList());

    final UserContext userContext =
        new UserContext(
            TfosConfig.DEFAULT_SUPERADMIN_ID,
            TfosConfig.DEFAULT_ORG_ID,
            TfosConfig.DEFAULT_SUPERADMIN_USERNAME,
            tenantName,
            permissionIds,
            "signupServiceHandler",
            AppType.INTERNAL);

    long currentTime = System.currentTimeMillis();
    final long expirationMillis = 120000; // finish within two minutes
    long expirationTime = currentTime + expirationMillis;

    final List<SignalConfig> regularSignals;
    final List<SignalConfig> auditSignals;
    final List<ContextConfig> materializedContexts;
    final List<ContextConfig> regularContexts;
    final String sessionToken = auth.createToken(currentTime, expirationTime, userContext);
    if (serviceType == ServiceType.SITE_ANALYTICS) {
      createAuditPageViewSignal(tenantConfig, tenantName, sessionToken, state);
      regularSignals = tenantConfig.getSignals();
      regularContexts = tenantConfig.getContexts();
      auditSignals = List.of();
      materializedContexts = List.of();
    } else {
      final var signals = tenantConfig.getSignals();
      final var contexts = tenantConfig.getContexts();
      regularSignals = new ArrayList<>();
      auditSignals = new ArrayList<>();
      regularContexts = new ArrayList<>();
      materializedContexts = new ArrayList<>();
      final var materializedContextNames = new HashSet<String>();
      if (signals != null) {
        for (var signal : signals) {
          if (signal.getPostStorageStage() != null
              && signal.getPostStorageStage().getFeatures() != null) {
            for (var feature : signal.getPostStorageStage().getFeatures()) {
              if (feature.getFeatureAsContextName() != null) {
                materializedContextNames.add(feature.getFeatureAsContextName().toLowerCase());
              }
            }
          }
          if (signal.getName().startsWith("audit")) {
            auditSignals.add(signal);
          } else {
            regularSignals.add(signal);
          }
        }
      }
      if (contexts != null) {
        for (var context : contexts) {
          if (materializedContextNames.contains(context.getName().toLowerCase())) {
            materializedContexts.add(context);
          } else {
            regularContexts.add(context);
          }
        }
      }
    }

    return AsyncTrampoline.asyncWhile(
            (i) -> i < materializedContexts.size(),
            (i) ->
                createContext(tenantName, materializedContexts.get(i), sessionToken, state)
                    .thenApply((context) -> i + 1),
            0)
        .thenComposeAsync(
            (x) ->
                AsyncTrampoline.asyncWhile(
                    (i) -> i < auditSignals.size(),
                    (i) ->
                        createSignal(tenantName, auditSignals.get(i), sessionToken, state)
                            .thenApply((signal) -> i + 1),
                    0))
        .thenComposeAsync(
            (x) ->
                AsyncTrampoline.asyncWhile(
                    (i) -> regularContexts != null && i < regularContexts.size(),
                    (i) ->
                        createContext(tenantName, regularContexts.get(i), sessionToken, state)
                            .thenApply((context) -> i + 1),
                    0))
        .thenComposeAsync(
            (x) ->
                AsyncTrampoline.asyncWhile(
                    (i) -> regularSignals != null && i < regularSignals.size(),
                    (i) ->
                        createSignal(tenantName, regularSignals.get(i), sessionToken, state)
                            .thenApply((signal) -> i + 1),
                    0))
        .thenComposeAsync(
            (x) ->
                AsyncTrampoline.asyncWhile(
                    (i) -> i < initialData.getContexts().size(),
                    (i) ->
                        upsertContext(
                                new SessionToken(sessionToken, false),
                                tenantName,
                                initialData.getContexts().get(i).getName(),
                                initialData.getContexts().get(i).getEntries(),
                                state)
                            .thenApply((signal) -> i + 1),
                    0));
  }

  private TenantConfig getStoreEnhancementSchema() {
    try (var inputStream =
        Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream("store_enhancement_schema.yaml")) {
      final var mapper = new ObjectMapper(new YAMLFactory());
      return mapper.readValue(inputStream, TenantConfig.class);
    } catch (IOException e) {
      throw new CompletionException(e);
    }
  }

  private TenantData getStoreEnhancementInitialData() {
    try (var inputStream =
        Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream("store_enhancement_initial_data.yaml")) {
      final var mapper = new ObjectMapper(new YAMLFactory());
      return mapper.readValue(inputStream, TenantData.class);
    } catch (IOException e) {
      throw new CompletionException(e);
    }
  }

  private TenantConfig getSiteAnalyticsSchema() {
    try (var inputStream =
        Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream("site_analytics_schema.yaml")) {
      final var mapper = new ObjectMapper(new YAMLFactory());
      return mapper.readValue(inputStream, TenantConfig.class);
    } catch (IOException e) {
      throw new CompletionException(e);
    }
  }

  private CompletionStage<Void> setupReports(
      String tenantName, ServiceType serviceType, UserContext userContext, ExecutionState state) {
    final ReportsConfig config;
    switch (serviceType) {
      case STORE_ENHANCEMENT:
        config = getStoreEnhancementReports();
        break;
      case SITE_ANALYTICS:
        config = getSiteAnalyticsReports();
        break;
      default:
        throw new UnsupportedOperationException(serviceType.toString());
    }

    return AsyncTrampoline.asyncWhile(
            (i) -> i < config.getReports().size(),
            (i) -> {
              final Map<String, Object> data = config.getReports().get(i);
              final Object reportId = data.get("reportId");
              if (reportId == null) {
                return CompletableFuture.completedFuture(i + 1);
              }
              return reports
                  .putReportConfigAsync(tenantName, userContext, reportId.toString(), data, state)
                  .thenApply((none) -> i + 1);
            },
            0)
        .thenComposeAsync(
            (x) ->
                AsyncTrampoline.asyncWhile(
                    (iterator) -> iterator.hasNext(),
                    (iterator) -> {
                      final var entry = iterator.next();
                      return reports
                          .putInsightConfigsAsync(
                              tenantName, entry.getKey(), userContext, entry.getValue(), state)
                          .thenApply((none) -> iterator);
                    },
                    config.getInsights().entrySet().iterator()),
            state.getExecutor())
        .thenAccept((x) -> {});
  }

  private ReportsConfig getStoreEnhancementReports() {
    try (var inputStream =
        Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream("store_enhancement_reports.yaml")) {
      final var mapper = new ObjectMapper(new YAMLFactory());
      return mapper.readValue(inputStream, ReportsConfig.class);
    } catch (IOException e) {
      throw new CompletionException(e);
    }
  }

  private ReportsConfig getSiteAnalyticsReports() {
    try (var inputStream =
        Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream("site_analytics_reports.yaml")) {
      final var mapper = new ObjectMapper(new YAMLFactory());
      return mapper.readValue(inputStream, ReportsConfig.class);
    } catch (IOException e) {
      throw new CompletionException(e);
    }
  }

  private String makeTenantName(String domain) {
    final String fundamentalName = domain.replace(".", "_").replace("-", "_");
    String tenantName = fundamentalName;
    final int fundamentalNameMaxLength = 34;
    if (tenantName.length() > fundamentalNameMaxLength) {
      tenantName = tenantName.substring(0, fundamentalNameMaxLength - 1);
    }
    int trialsLeft = 5;
    while (trialsLeft-- > 0) {
      try {
        tfosAdmin.getTenant(tenantName);
        // The name is occupied
        tenantName = fundamentalName + "_" + RandomStringUtils.random(5, false, true);
      } catch (NoSuchTenantException e) {
        // The name is available for a new tenant
        return tenantName;
      }
    }
    // It's unlikely to happen but use pure random string in this case
    return RandomStringUtils.random(8, true, false);
  }
}
