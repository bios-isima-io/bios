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
package io.isima.bios.framework;

import io.isima.bios.admin.Admin;
import io.isima.bios.admin.ResourceAllocator;
import io.isima.bios.admin.TenantAppendix;
import io.isima.bios.admin.TenantAppendixImpl;
import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.AdminStore;
import io.isima.bios.admin.v1.DomainResolver;
import io.isima.bios.admin.v1.SharedConfig;
import io.isima.bios.admin.v1.impl.AdminImpl;
import io.isima.bios.admin.v1.impl.SharedPropertiesImpl;
import io.isima.bios.admin.v1.store.impl.AdminStoreImpl;
import io.isima.bios.apps.AppsChangeNotifier;
import io.isima.bios.apps.AppsManager;
import io.isima.bios.audit.AuditManager;
import io.isima.bios.audit.AuditManagerImpl;
import io.isima.bios.auth.Auth;
import io.isima.bios.auth.AuthImpl;
import io.isima.bios.auth.v1.AuthV1;
import io.isima.bios.auth.v1.impl.AuthV1Impl;
import io.isima.bios.bi.Reports;
import io.isima.bios.bi.ReportsImpl;
import io.isima.bios.bi.teachbios.TeachBios;
import io.isima.bios.bi.teachbios.namedetector.NameDetector;
import io.isima.bios.bi.teachbios.namedetector.NameMatchTieBreaker;
import io.isima.bios.bi.teachbios.namedetector.PriorityBasedTieBreaker;
import io.isima.bios.bi.teachbios.namedetector.RegexBasedNameDetector;
import io.isima.bios.bi.teachbios.typedetector.TypeCastBasedTypeDetector;
import io.isima.bios.bi.teachbios.typedetector.TypeDetector;
import io.isima.bios.common.BiosConstants;
import io.isima.bios.common.SharedProperties;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.data.DataEngine;
import io.isima.bios.data.QueryLogger;
import io.isima.bios.data.impl.DataEngineImpl;
import io.isima.bios.data.impl.GlobalContextRepository;
import io.isima.bios.errors.exception.FileReadException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutorManager;
import io.isima.bios.execution.digestor.Digestor;
import io.isima.bios.export.DataExportWorker;
import io.isima.bios.export.ExportServiceImpl;
import io.isima.bios.healthcheck.HealthCheckLogger;
import io.isima.bios.mail.MailClient;
import io.isima.bios.mail.impl.MailClientImpl;
import io.isima.bios.maintenance.Maintenance;
import io.isima.bios.maintenance.ServiceStatus;
import io.isima.bios.maintenance.SystemMonitor;
import io.isima.bios.maintenance.WorkerLock;
import io.isima.bios.metrics.ClientMetricsStream;
import io.isima.bios.metrics.Ip2GeoStream;
import io.isima.bios.metrics.IpBlacklistStream;
import io.isima.bios.metrics.MetricsStreamProvider;
import io.isima.bios.metrics.OperationFailureStream;
import io.isima.bios.metrics.OperationMetrics;
import io.isima.bios.repository.auth.DomainRepository;
import io.isima.bios.repository.auth.GroupRepository;
import io.isima.bios.repository.auth.OrganizationRepository;
import io.isima.bios.repository.auth.UserRepository;
import io.isima.bios.server.handlers.DataServiceHandler;
import io.isima.bios.service.HttpClientManager;
import io.isima.bios.service.InferredTagsDistributor;
import io.isima.bios.service.JupyterHubAdmin;
import io.isima.bios.service.handler.AdminServiceHandler;
import io.isima.bios.service.handler.AuthServiceHandler;
import io.isima.bios.service.handler.BiServiceHandler;
import io.isima.bios.service.handler.ExportServiceHandler;
import io.isima.bios.service.handler.InsertServiceHandler;
import io.isima.bios.service.handler.SignupServiceHandler;
import io.isima.bios.service.handler.TestServiceHandler;
import io.isima.bios.service.handler.UserManagementServiceHandler;
import io.isima.bios.storage.cassandra.CassandraConnection;
import io.isima.bios.upgrade.UpgradeSystem;
import io.isima.bios.user.TfosUserManager;
import io.isima.bios.user.UserManager;
import io.isima.bios.utils.Utils;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Stack;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/** Modules inside the new BiosServer application. */
@Slf4j
@ToString
public class BiosModules {

  static BiosModules instance;

  // Components
  private CassandraConnection cassandraConnection;
  private AuthV1 authV1;
  private AdminStore adminStore;
  private DomainResolver domainResolver;
  private AdminImpl adminV1;
  private DataEngineImpl dataEngine;
  private QueryLogger queryLogger;
  private SharedProperties sharedProperties;
  private SharedConfig sharedConfig;
  private ServiceStatus serviceStatus;
  private TeachBios teachBios;
  private Maintenance maintenance;
  private WorkerLock workerLock;
  private OperationMetrics metrics;
  private SystemMonitor systemMonitor;
  AuditManager auditManager;
  private HealthCheckLogger healthCheckLogger;

  private OrganizationRepository organizationRepository;
  private GroupRepository groupRepository;
  private UserRepository userRepository;
  private DomainRepository domainRepository;
  private MailClient mailClient;

  Auth auth;
  UserManager userManager;
  TenantAppendix tenantAppendix;
  Admin admin;
  Reports reports;
  HttpClientManager httpClientManager;
  ResourceAllocator resourceAllocator;
  AppsManager appsManager;
  AppsChangeNotifier appsChangeNotifier;
  InferredTagsDistributor inferredTagsDistributor;
  GlobalContextRepository globalContextRepository;

  private final Stack<BiosModule> modules;

  private ExecutorManager executorManager;
  private Digestor digestor;

  // Service Handlers
  AdminServiceHandler adminServiceHandler;
  AuthServiceHandler authServiceHandler;
  BiServiceHandler biServiceHandler;
  ExportServiceHandler exportServiceHandler;
  InsertServiceHandler insertServiceHandler;
  TestServiceHandler testServiceHandler;
  UserManagementServiceHandler userManagementServiceHandler;
  JupyterHubAdmin jupyterHubAdmin;
  SignupServiceHandler signUpServiceHandler;

  private DataServiceHandler dataServiceHandler;

  // Controlling parameters
  private boolean isTestMode = false;
  private boolean isStarted = false;

  // for testing
  @Getter private String modulesName;

  /**
   * Starts BiosServer server modules.
   *
   * @param properties Server configuration properties to take.
   * @throws ApplicationException thrown to indicate that an unexpected error happens during the
   *     startup.
   */
  public static void startModules(Properties properties) {
    logger.info("Initializing biOS modules");
    TfosConfig.setProperties(properties);
    instance = new BiosModules();
    instance.startComponents(false, Map.of());
    instance.startServiceHandlers();
    logger.info("All BiOS modules started");
  }

  /**
   * Starts BiosServer server modules for testing.
   *
   * @param properties Server configuration properties to take.
   * @return previous shared property values
   * @throws ApplicationException thrown to indicate that an unexpected error happens during the
   *     startup.
   */
  public static Map<String, String> startModulesForTesting(
      Properties properties,
      boolean runMaintenance,
      Object testId,
      Map<String, String> initialSharedProperties) {
    logger.info("Initializing biOS modules");
    TfosConfig.setProperties(properties);
    instance = new BiosModules();
    setTestMode(true);
    final var previousSharedProperties =
        instance.startComponents(runMaintenance, initialSharedProperties);
    instance.modulesName = Objects.requireNonNullElse(testId, "unknown").toString();
    instance.startServiceHandlers();
    logger.info("All BiOS modules started");
    return previousSharedProperties;
  }

  public static BiosModules makeDummyModulesForTesting(Object testClass) {
    final var modules = new BiosModules();
    modules.modulesName = testClass.toString();
    return modules;
  }

  /** Shuts down all modules. */
  public static void shutdown() {
    if (instance != null) {
      if (instance.dataEngine != null && instance.dataEngine.getMaintenance() != null) {
        instance.dataEngine.getMaintenance().shutdown();
      }
      instance.shutdownModules();
      if (instance.exportServiceHandler != null) {
        instance.exportServiceHandler.stop();
      }
      if (instance.digestor != null) {
        instance.digestor.shutdown();
      }
      if (instance.executorManager != null) {
        instance.executorManager.shutdown();
      }
      instance.isStarted = false;
    }
  }

  private static BiosModules getInstance() {
    if (instance == null) {
      throw new IllegalStateException("The biOS modules have not started yet");
    }
    return instance;
  }

  private BiosModules() {
    // Set biOS modules
    this.modules = new Stack<>();
  }

  /**
   * Method to start all TFOS modules and possibly maintenance tasks.
   *
   * @param runMaintenance The method starts maintenance tasks if true.
   * @param initialSharedProperties Shared properties to be set before starting the modules. This
   *     parameter is used for testing.
   * @return Previous shared properties of specified ones
   */
  private Map<String, String> startComponents(
      boolean runMaintenance, Map<String, String> initialSharedProperties) {
    final var previousProperties = new HashMap<String, String>();
    try {
      executorManager = new ExecutorManager();
      digestor =
          new Digestor(
              executorManager.getDigestorNumThreads(),
              executorManager.createDigestorThreadFactory());
      cassandraConnection = new CassandraConnection();

      sharedProperties = new SharedPropertiesImpl(cassandraConnection);
      SharedProperties.setInstance(sharedProperties);

      sharedConfig = new SharedConfig(sharedProperties);
      sharedConfig.migrateEndpointToNode();

      // Set initial properties here before starting up other modules
      for (var entry : initialSharedProperties.entrySet()) {
        previousProperties.put(entry.getKey(), sharedProperties.getProperty(entry.getKey()));
        sharedProperties.setProperty(entry.getKey(), entry.getValue());
      }
      SharedProperties.clearCache();

      serviceStatus = new ServiceStatus(sharedConfig);

      workerLock = new WorkerLock();
      maintenance = new Maintenance();

      organizationRepository = new OrganizationRepository(cassandraConnection);
      groupRepository = new GroupRepository(cassandraConnection);
      userRepository = new UserRepository(cassandraConnection);
      domainRepository = new DomainRepository(cassandraConnection);

      final var authImpl =
          new AuthV1Impl(organizationRepository, userRepository, groupRepository, sharedConfig);
      authV1 = authImpl;
      final var userManagerImpl =
          new TfosUserManager(authV1, userRepository, organizationRepository);
      userManager = userManagerImpl;

      DataEngineImpl dataEngineImpl =
          new DataEngineImpl(
              cassandraConnection, serviceStatus, sharedConfig, sharedProperties, this);
      dataEngine = dataEngineImpl;
      maintenance.register(dataEngineImpl.getMaintenanceWorker());

      adminStore = new AdminStoreImpl(cassandraConnection);
      metrics = new OperationMetrics(dataEngine);
      final var metricsStreamProvider = new MetricsStreamProvider();
      final var ip2GeoStreamProvider = new Ip2GeoStream();
      final var ipBlacklistStreamProvider = new IpBlacklistStream();
      final var clientMetricsStreamProvider = new ClientMetricsStream();
      final var OperationFailureStreamProvider = new OperationFailureStream();
      domainResolver = new DomainResolver(organizationRepository);
      adminV1 =
          new AdminImpl(
              adminStore,
              metricsStreamProvider,
              ip2GeoStreamProvider,
              ipBlacklistStreamProvider,
              clientMetricsStreamProvider,
              OperationFailureStreamProvider,
              dataEngineImpl,
              userManagerImpl,
              domainResolver,
              metrics);
      queryLogger = new QueryLogger(dataEngine, adminV1);
      dataEngineImpl.setQueryLogger(queryLogger);

      // now is a good time to upgrade
      final var upgradeSystem = new UpgradeSystem(sharedProperties, adminV1, Utils.getNodeName());
      upgradeSystem.upgrade();

      userManagerImpl.systemUsersBootstrap();

      auditManager = new AuditManagerImpl(dataEngine);

      systemMonitor = new SystemMonitor(serviceStatus, adminV1, dataEngine, this);
      maintenance.register(systemMonitor);
      healthCheckLogger = new HealthCheckLogger(BiosConstants.APP_TFOS, this);
      maintenance.register(healthCheckLogger);
      healthCheckLogger.registerObserver(systemMonitor);

      NameMatchTieBreaker nameMatchTieBreaker = new PriorityBasedTieBreaker();
      final TypeDetector typeDetector = new TypeCastBasedTypeDetector();
      final NameDetector nameDetector = new RegexBasedNameDetector(nameMatchTieBreaker);
      teachBios = new TeachBios(typeDetector, nameDetector);

      mailClient = new MailClientImpl();

      auth = new AuthImpl(authImpl, sharedConfig);
      tenantAppendix = new TenantAppendixImpl(cassandraConnection);
      resourceAllocator = new ResourceAllocator(cassandraConnection);
      appsManager = new AppsManager(tenantAppendix, resourceAllocator, sharedConfig);
      admin = new io.isima.bios.admin.AdminImpl(adminV1, tenantAppendix, appsManager);
      reports = new ReportsImpl(adminV1, cassandraConnection);
      appsChangeNotifier = new AppsChangeNotifier(appsManager);
      httpClientManager = new HttpClientManager();
      inferredTagsDistributor = new InferredTagsDistributor(auth, httpClientManager);
      globalContextRepository = GlobalContextRepository.getInstance();

      if (runMaintenance) {
        maintenance.start();
      }

    } catch (FileReadException ex) {
      logger.info("Failed to start service due to error in reading file", ex);
      shutdown();
      throw new RuntimeException(ex.getCause());
    } catch (ApplicationException ex) {
      logger.error("Failed to start service, shutting down", ex);
      shutdown();
      throw new RuntimeException(ex.getCause());
    } catch (Throwable t) {
      logger.error("Failed to start service, shutting down", t);
      shutdown();
      throw new RuntimeException(t);
    }
    isStarted = true;

    return previousProperties;
  }

  private void startServiceHandlers() {
    authServiceHandler =
        new AuthServiceHandler(this.auth, this.auditManager, adminV1, dataEngine, metrics);
    biServiceHandler =
        new BiServiceHandler(auditManager, this.auth, adminV1, dataEngine, this.reports, metrics);
    insertServiceHandler =
        new InsertServiceHandler(auditManager, this.auth, adminV1, dataEngine, metrics);
    adminServiceHandler =
        new AdminServiceHandler(
            auditManager,
            this.admin,
            adminV1,
            this.tenantAppendix,
            this.auth,
            dataEngine,
            metrics,
            sharedConfig,
            appsManager,
            appsChangeNotifier,
            teachBios);
    testServiceHandler =
        new TestServiceHandler(auditManager, this.auth, adminV1, dataEngine, metrics);
    userManagementServiceHandler =
        new UserManagementServiceHandler(
            userRepository,
            userManager,
            this.auth,
            adminV1,
            dataEngine,
            metrics,
            auditManager,
            mailClient);
    jupyterHubAdmin = new JupyterHubAdmin();

    signUpServiceHandler =
        new SignupServiceHandler(
            mailClient,
            userRepository,
            organizationRepository,
            auditManager,
            auth,
            adminV1,
            dataEngine,
            metrics,
            adminServiceHandler,
            this.httpClientManager,
            workerLock,
            reports);

    final var exportWorker = new DataExportWorker(serviceStatus, this);
    final var exportService =
        new ExportServiceImpl(admin, tenantAppendix, dataEngine, exportWorker);
    exportServiceHandler =
        new ExportServiceHandler(auditManager, auth, adminV1, dataEngine, metrics, exportService);
    adminV1.injectExportService(exportService);
    maintenance.register(exportWorker);
    this.dataServiceHandler =
        new DataServiceHandler(
            getDataEngine(), getAdminInternal(), getMetrics(), getSharedConfig());

    this.digestor.register(getMetrics());
  }

  /**
   * Register a module. The pointers are kept in modules stack that is used when shutting down.
   *
   * @param module Module to register
   */
  private <T extends BiosModule> T register(T module) {
    modules.push(module);
    return module;
  }

  /**
   * Shutdown modules.
   *
   * <p>This method shuts down registered modules in the reverse order of registration.
   */
  private void shutdownModules() {
    logger.info("Shutting down biOS modules");
    while (!modules.isEmpty()) {
      modules.pop().shutdown();
    }

    if (workerLock != null) {
      workerLock.shutdown();
      logger.info("** WorkerLock has stopped");
    }
    if (maintenance != null) {
      maintenance.shutdown();
      logger.info("** Maintenance has stopped");
    }
    if (serviceStatus != null) {
      serviceStatus.enterMaintenanceMode();
      logger.info("** The server entered maintenance mode");
    }
    if (metrics != null) {
      metrics.shutdown();
      logger.info("** Metrics has stopped");
    }
    if (dataEngine != null) {
      dataEngine.shutdown();
      logger.info("** DataEngine has stopped");
    }
    if (cassandraConnection != null) {
      logger.info("oo CassandraConnection is shutting down");
      cassandraConnection.shutdown();
      logger.info("** CassandraConnection has stopped");
    }

    logger.info("biOS modules have shut down");
  }

  // Methods to get server modules ////////////////////////////

  public static ExecutorManager getExecutorManager() {
    return getInstance().executorManager;
  }

  public static Digestor getDigestor() {
    return getInstance().digestor;
  }

  public static DataServiceHandler getDataServiceHandler() {
    return getInstance().dataServiceHandler;
  }

  public static CassandraConnection getCassandraConnection() {
    return instance.cassandraConnection;
  }

  public static Maintenance getMaintenance() {
    return instance.maintenance;
  }

  public static AuthV1 getAuthV1() {
    return instance.authV1;
  }

  public static AdminInternal getAdminInternal() {
    return instance.adminV1;
  }

  public static AdminStore getAdminStore() {
    return instance.adminStore;
  }

  public static SharedProperties getSharedProperties() {
    return instance.sharedProperties;
  }

  public static SharedConfig getSharedConfig() {
    return instance.sharedConfig;
  }

  public static DataEngine getDataEngine() {
    return instance.dataEngine;
  }

  public static TeachBios getTeachBios() {
    return instance.teachBios;
  }

  public static OperationMetrics getMetrics() {
    return instance.metrics;
  }

  public static WorkerLock getWorkerLock() {
    return instance.workerLock;
  }

  public static SystemMonitor getSystemMonitor() {
    return instance.systemMonitor;
  }

  public static OrganizationRepository getOrganizationRepository() {
    return instance.organizationRepository;
  }

  public static GroupRepository getGroupRepository() {
    return instance.groupRepository;
  }

  public static UserRepository getUserRepository() {
    return instance.userRepository;
  }

  public static DomainResolver getDomainResolver() {
    return instance.domainResolver;
  }

  public static MailClient getMailClient() {
    return instance.mailClient;
  }

  public static AuditManager getAuditManager() {
    return instance.auditManager;
  }

  public static ServiceStatus getServiceStatus() {
    return instance.serviceStatus;
  }

  public static void setProperties(Properties properties) {
    TfosConfig.setProperties(properties);
  }

  public static io.isima.bios.data.QueryLogger getQueryLogger() {
    return instance.queryLogger;
  }

  public static boolean isStarted() {
    return instance != null && instance.isStarted;
  }

  public static boolean isTestMode() {
    return instance.isTestMode;
  }

  public static void setTestMode(boolean isTestMode) {
    instance.isTestMode = isTestMode;
  }

  public static Admin getAdmin() {
    return instance.admin;
  }

  public static TenantAppendix getTenantAppendix() {
    return instance.tenantAppendix;
  }

  public static Auth getAuth() {
    return instance.auth;
  }

  public static UserManager getUserManager() {
    return instance.userManager;
  }

  public static Reports getReports() {
    return instance.reports;
  }

  public static AdminServiceHandler getAdminServiceHandler() {
    return instance.adminServiceHandler;
  }

  public static AuthServiceHandler getAuthServiceHandler() {
    return instance.authServiceHandler;
  }

  public static BiServiceHandler getBiServiceHandler() {
    return instance.biServiceHandler;
  }

  public static ExportServiceHandler getExportServiceHandler() {
    return instance.exportServiceHandler;
  }

  public static InsertServiceHandler getInsertServiceHandler() {
    return instance.insertServiceHandler;
  }

  public static UserManagementServiceHandler getUserManagementServiceHandler() {
    return instance.userManagementServiceHandler;
  }

  public static JupyterHubAdmin getJupyterHubAdmin() {
    return instance.jupyterHubAdmin;
  }

  public static SignupServiceHandler getSignUpServiceHandler() {
    return instance.signUpServiceHandler;
  }

  public static GlobalContextRepository getGlobalContextRepository() {
    return instance.globalContextRepository;
  }

  public static TestServiceHandler getTestServiceHandler() {
    return instance.testServiceHandler;
  }

  public static HttpClientManager getHttpClientManager() {
    return instance.httpClientManager;
  }

  public static ResourceAllocator getResourceAllocator() {
    return instance.resourceAllocator;
  }

  public static AppsManager getAppsManager() {
    return instance.appsManager;
  }

  public static AppsChangeNotifier getIntegrationFlowChangeNotifier() {
    return instance.appsChangeNotifier;
  }

  public static InferredTagsDistributor getInferredTagsDistributor() {
    return instance.inferredTagsDistributor;
  }
}
