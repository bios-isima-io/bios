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
package io.isima.bios.user;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.admin.v1.AdminChangeListener;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.auth.v1.AuthV1;
import io.isima.bios.common.BiosConstants;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.errors.UserError;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.ExecutorManager;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.Role;
import io.isima.bios.models.UserConfig;
import io.isima.bios.models.UserContext;
import io.isima.bios.models.auth.Organization;
import io.isima.bios.models.auth.User;
import io.isima.bios.models.v1.Permission;
import io.isima.bios.repository.auth.OrganizationRepository;
import io.isima.bios.repository.auth.UserRepository;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Stack;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TfosUserManager implements UserManager, AdminChangeListener {
  private static final Logger logger = LoggerFactory.getLogger(TfosUserManager.class);
  private static final ObjectMapper objectMapper = BiosObjectMapperProvider.get();

  private final AuthV1 auth;
  private final UserRepository userRepository;
  private final OrganizationRepository organizationRepository;

  private final Stack<UserConfig> systemUsers;
  private final List<UserConfig> initialTenantUsers;

  private final SecureRandom randomGenerator;

  public TfosUserManager(
      AuthV1 auth, UserRepository userRepository, OrganizationRepository organizationRepository) {
    this.auth = auth;
    this.userRepository = userRepository;
    this.organizationRepository = organizationRepository;

    systemUsers = new Stack<>();
    initialTenantUsers = new ArrayList<>();

    randomGenerator = new SecureRandom();

    bootstrap();
  }

  private void bootstrap() {
    try {
      final var state = new GenericExecutionState("bootstrap", Executors.newSingleThreadExecutor());
      // Fetch or create the default organization
      var defaultOrganization =
          organizationRepository.findById(TfosConfig.DEFAULT_ORG_ID, state).get();
      if (defaultOrganization == null) {
        logger.debug("default org not exists, creating org");
        defaultOrganization =
            organizationRepository
                .setupOrg(TfosConfig.DEFAULT_ORG_ID, "platform", "/", state)
                .get();
      }
      if (defaultOrganization == null) {
        throw new RuntimeException("Failed to create the default organization");
      }

      // Load initial users
      UserConfig[] users = loadInitialUsers();
      long currentUserId = TfosConfig.DEFAULT_SUPERADMIN_ID;

      // Sort user configs. The first entry with the tenant name being set is considered to be the
      // default system admin. A user who does not have tenant property is considered to be an
      // initial tenant user.
      for (UserConfig user : users) {
        if (user.getTenantName() != null) {
          user.setUserId(Long.toString(currentUserId++));
          systemUsers.push(user);
        } else {
          initialTenantUsers.add(user);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Bootstrap interrupted", e);
    } catch (Throwable ex) {
      throw new RuntimeException("Failed to bootstrap", ex);
    }
  }

  // UserManager methods /////////////////////////////////////////////////////

  @Override
  public void systemUsersBootstrap() {
    try {
      // System users bootstrap is done already if the default system admin user exists
      final var state =
          new GenericExecutionState("systemUsersBootstrap", ExecutorManager.getSidelineExecutor());
      final var tfosSuperAdmin =
          userRepository
              .findById(TfosConfig.DEFAULT_ORG_ID, TfosConfig.DEFAULT_SUPERADMIN_ID, state)
              .get();
      if (tfosSuperAdmin != null) {
        logger.debug("superadmin user exists already");
        return;
      }

      // Create the initial users
      while (!systemUsers.isEmpty()) {
        createUser(systemUsers.pop());
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Bootstrap interrupted", e);
    } catch (Throwable ex) {
      throw new RuntimeException("Failed to bootstrap", ex);
    }
  }

  @Override
  public CompletableFuture<User> createUserAsync(UserConfig userConfig, ExecutionState state) {
    Objects.requireNonNull(userConfig);
    Objects.requireNonNull(userConfig.getTenantName());
    Objects.requireNonNull(userConfig.getEmail());
    Objects.requireNonNull(userConfig.getPassword());
    Objects.requireNonNull(userConfig.getRoles());

    final var email = userConfig.getEmail();
    final String tenantName;
    if (BiosConstants.TENANT_SYSTEM.equalsIgnoreCase(userConfig.getTenantName())) {
      tenantName = "/";
    } else {
      tenantName = userConfig.getTenantName();
    }

    final var error = checkIfAppMaster(userConfig, state);
    if (error != null) {
      return CompletableFuture.failedFuture(error);
    }
    logger.info("Creating user {}; tenant={}", userConfig.getEmail(), userConfig.getTenantName());

    return userRepository
        .findByEmail(email, state)
        .thenAccept(
            (user) -> {
              if (user != null) {
                throw new CompletionException(
                    new TfosException(UserError.USER_ALREADY_EXISTS, email));
              }
            })
        .thenComposeAsync(
            (none) -> {
              return organizationRepository.findByTenantName(tenantName, state);
            })
        .thenComposeAsync(
            (organization) -> {
              if (organization == null) {
                throw new CompletionException(new NoSuchTenantException(tenantName));
              }
              final Long userId =
                  userConfig.getUserId() != null
                      ? Long.valueOf(userConfig.getUserId())
                      : generateUniqueId();
              final String userName;
              if (userConfig.getFullName() != null) {
                userName = userConfig.getFullName();
              } else {
                userName = userConfig.getEmail().split("@")[0];
              }
              final String passwordSrc = userConfig.getPassword();
              final String passwordHash =
                  (userConfig.getPasswordHashed() != null && userConfig.getPasswordHashed())
                      ? passwordSrc
                      : auth.hash(passwordSrc);
              final var permissions =
                  userConfig.getRoles().stream()
                      .map((role) -> Permission.forBios(role))
                      .collect(Collectors.toList());
              return userRepository.setupUser(
                  organization.getId(),
                  userId,
                  userName,
                  userConfig.getEmail(),
                  passwordHash,
                  permissions,
                  userConfig.getStatus(),
                  state);
            });
  }

  @Override
  public CompletableFuture<UserConfig> modifyUserAsync(
      Long userId, UserConfig userConfig, UserContext requestor, ExecutionState state) {
    Objects.requireNonNull(userId);
    Objects.requireNonNull(userConfig);
    Objects.requireNonNull(requestor);

    final Long requestorOrgId;
    if (BiosConstants.TENANT_SYSTEM.equalsIgnoreCase(requestor.getTenant())) {
      requestorOrgId = null;
    } else {
      requestorOrgId = requestor.getOrgId();
    }

    final var error = checkIfAppMaster(userConfig, state);
    if (error != null) {
      return CompletableFuture.failedFuture(error);
    }

    return userRepository
        .findById(requestorOrgId, userId, true, state)
        .thenCompose(
            (user) -> {
              if (user == null || !checkUserScope(user, requestor)) {
                throw new CompletionException(
                    new TfosException(UserError.NO_SUCH_USER, userId.toString()));
              }
              if (userConfig.getFullName() != null) {
                user.setName(userConfig.getFullName());
              }
              if (userConfig.getStatus() != null) {
                user.setStatus(userConfig.getStatus());
              }
              if (userConfig.getRoles() != null) {
                final String permissionIds =
                    userConfig.getRoles().stream()
                        .map(Permission::forBios)
                        .map(Permission::getId)
                        .map((id) -> Integer.toString(id))
                        .collect(Collectors.joining(","));
                user.setPermissions(permissionIds);
              }
              user.setUpdatedAt(new Date());
              return userRepository.save(user, state);
            })
        .thenCompose(
            (user) -> {
              final var modifiedUserConfig = user.toUserConfig();
              if (requestorOrgId != null) {
                modifiedUserConfig.setTenantName(requestor.getTenant());
                return CompletableFuture.completedFuture(modifiedUserConfig);
              }
              return organizationRepository
                  .findById(user.getOrgId(), state)
                  .thenApply(
                      (organization) -> {
                        modifiedUserConfig.setTenantName(organization.getTenant());
                        return modifiedUserConfig;
                      });
            });
  }

  @Override
  public CompletableFuture<Void> deleteUserAsync(
      String email, Long userId, UserContext requestor, ExecutionState state) {
    final CompletableFuture<User> initialStage;
    if (userId != null) {
      initialStage = userRepository.findById(null, userId, true, state);
    } else {
      initialStage = userRepository.findByEmail(email, state);
    }
    return initialStage.thenComposeAsync(
        (user) -> {
          if (user == null || !checkUserScope(user, requestor)) {
            throw new CompletionException(new TfosException(UserError.NO_SUCH_USER, email));
          }
          return userRepository.remove(null, user.getId(), state);
        });
  }

  private boolean checkUserScope(User user, UserContext requestor) {
    Objects.requireNonNull(user);
    Objects.requireNonNull(requestor);
    return requestor.getPermissions().contains(Permission.SUPERADMIN)
        || requestor.getOrgId().equals(user.getOrgId());
  }

  // AdminChangeListener methods ////////////////////////////////////////////

  @Override
  public void createTenant(TenantDesc tenantDesc, RequestPhase phase) throws ApplicationException {
    if (phase == RequestPhase.INITIAL) {
      String domain = tenantDesc.getDomain();
      if (domain == null) {
        domain = tenantDesc.getName();
      }
      setupTenant(tenantDesc.getName(), domain);
    }
  }

  @Override
  public void deleteTenant(TenantDesc tenantDesc, RequestPhase phase)
      throws NoSuchTenantException, ApplicationException {
    if (phase == RequestPhase.INITIAL) {
      String tenantName = tenantDesc.getName().toLowerCase();
      tearDownTenant(tenantName);
    }
  }

  @Override
  public void createStream(String tenantName, StreamDesc streamDesc, RequestPhase phase) {
    // do nothing
  }

  @Override
  public void deleteStream(String tenantName, StreamDesc streamDesc, RequestPhase phase) {
    // do nothing
  }

  @Override
  public void unload() {
    // do nothing
  }

  // Utilities ////////////////////////////////////////////////////////////////////////

  /**
   * Load initial users from the users JSON file.
   *
   * @return Array of users.
   * @throws IOException thrown to indicate loading fails.
   */
  private UserConfig[] loadInitialUsers() throws IOException {
    try (InputStream inputStream = getInitialUserStream()) {
      return objectMapper.readValue(inputStream, UserConfig[].class);
    }
  }

  /**
   * Open initial user file as InputStream if the property is set, otherwise open the default
   * initial user file.
   *
   * @return Initial user file as InputStream.
   * @throws IOException when fialed to open the file.
   */
  private InputStream getInitialUserStream() throws IOException {
    final String initialUserFilePath = TfosConfig.initialUsersFile();
    if (initialUserFilePath != null) {
      File initialFile = new File(initialUserFilePath);
      return new FileInputStream(initialFile);
    }

    final var stream =
        Thread.currentThread().getContextClassLoader().getResourceAsStream("initial_users.json");
    if (stream == null) {
      throw new IOException("Default initial users file not found");
    }
    return stream;
  }

  private Throwable checkIfAppMaster(UserConfig userConfig, ExecutionState state) {
    if (userConfig.getRoles() != null && userConfig.getRoles().contains(Role.APP_MASTER)) {
      if (state.getUserContext() == null) {
        return new ApplicationException(
            "UserContext must be set to execution state to create an AppMaster user");
      }
      if (!state.getUserContext().getPermissions().contains(Permission.SUPERADMIN)) {
        return new TfosException(
            UserError.ROLE_NOT_ALLOWED, "Only System Admin users can create an AppMaster user");
      }
    }
    return null;
  }

  private User createUser(UserConfig user) throws Throwable {
    try {
      final var state =
          new GenericExecutionState("createUser", ExecutorManager.getSidelineExecutor());
      return createUserAsync(user, state).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  private void setupTenant(String tenantName, String domain) throws ApplicationException {

    if (BiosConstants.TENANT_SYSTEM.equalsIgnoreCase(tenantName)) {
      // It's the default system tenant. We have it as tenant "/" already.
      return;
    }

    final var state =
        new GenericExecutionState("setupTenant", ExecutorManager.getSidelineExecutor());

    try {
      Organization organization = organizationRepository.findByTenantName(tenantName, state).get();
      if (organization == null) {
        organization =
            organizationRepository.setupOrg(generateUniqueId(), domain, tenantName, state).get();
      }
      if (organization == null) {
        throw new RuntimeException(
            String.format("failed to create org for new tenant: tenantName=%s", domain));
      }

      initialTenantUsers.stream()
          .map(
              (user) -> {
                final var clone = new UserConfig(user);
                clone.setTenantName(tenantName);
                clone.setEmail(clone.getEmail().replace("${tenantName}", tenantName));
                clone.setFullName(clone.getFullName().replace("${tenantName}", tenantName));
                return createUserAsync(clone, state);
              })
          .forEach(
              (future) -> {
                try {
                  future.get();
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new RuntimeException(e);
                } catch (ExecutionException e) {
                  throw new RuntimeException(e);
                }
              });

    } catch (InterruptedException ex) {
      logger.error("Bootstrap interrupted", ex);
      Thread.currentThread().interrupt();
      throw new ApplicationException("Failed to create user for new tenant", ex);
    } catch (ExecutionException | RuntimeException ex) {
      logger.error("Failed to bootstrap, as users creation failed", ex);
      throw new ApplicationException("Failed to create user for new tenant", ex);
    }
  }

  private void tearDownTenant(String tenantName) throws ApplicationException {
    Organization existingOrg = null;
    final var state =
        new GenericExecutionState("tearDownTenant", ExecutorManager.getSidelineExecutor());

    try {
      logger.info("removing tenant {} - find org linked to tenant", tenantName);
      existingOrg = organizationRepository.findByTenantName(tenantName, state).get();
    } catch (InterruptedException e) {
      logger.error("Interrupted while getting organization for removing tenant", e);
      Thread.currentThread().interrupt();
      throw new ApplicationException("Failed to get organization for removing tenant", e);
    } catch (ExecutionException e) {
      logger.error("Failed to get organization for removing tenant", e);
      throw new ApplicationException("Failed to get organization for removing tenant", e);
    }

    if (existingOrg == null) {
      return;
    }

    // find all the users belong to the organization
    List<User> users;
    try {
      logger.debug("removing tenant {} - find users linked to tenant", tenantName);
      users = userRepository.findAllByOrg(existingOrg.getId(), null, state).get();
    } catch (InterruptedException e) {
      logger.error("Interrupted while getting users for removing tenant", e);
      Thread.currentThread().interrupt();
      throw new ApplicationException("Failed to get users for removing tenant", e);
    } catch (ExecutionException e) {
      logger.error("Failed to get users for removing tenant", e);
      throw new ApplicationException("Failed to get users for removing tenant", e);
    }

    if (users == null || users.isEmpty()) {
      return;
    }

    // remove all the users
    List<Long> userIds = new ArrayList<>(users.size());
    users.forEach(user -> userIds.add(user.getId()));
    try {
      logger.info("removing tenant {} - removing users linked to tenant", tenantName);
      userRepository.remove(existingOrg.getId(), userIds, state).get();
    } catch (InterruptedException e) {
      logger.error("Interrupted while deleting users for removing tenant", e);
      Thread.currentThread().interrupt();
      throw new ApplicationException("Failed to delete users for removing tenant", e);
    } catch (ExecutionException e) {
      logger.error("Failed to delete users for removing tenant", e);
      throw new ApplicationException("Failed to delete users for removing tenant", e);
    }

    // now remove the organization
    try {
      logger.info("removing tenant {} - removing org linked to tenant", tenantName);
      organizationRepository.remove(existingOrg.getId(), state).get();
    } catch (InterruptedException e) {
      logger.error("Interrupted deleting organization for removing tenant", e);
      Thread.currentThread().interrupt();
      throw new ApplicationException("Failed to delete organization for removing tenant", e);
    } catch (ExecutionException e) {
      logger.error("Failed to delete organization for removing tenant", e);
      throw new ApplicationException("Failed to delete organization for removing tenant", e);
    }
  }

  public static long generateUniqueId() {
    final var id = System.currentTimeMillis();
    try {
      Thread.sleep(1);
    } catch (InterruptedException e) {
      // It's ok
      Thread.currentThread().interrupt();
    }
    return id;
  }
}
