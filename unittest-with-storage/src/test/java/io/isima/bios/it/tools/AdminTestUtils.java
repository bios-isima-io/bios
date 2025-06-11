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
package io.isima.bios.it.tools;

import static io.isima.bios.admin.v1.AdminConstants.CONTEXT_AUDIT_SIGNAL_PREFIX;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.admin.prerequisites.ContextAudit;
import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.auth.Auth;
import io.isima.bios.data.impl.DataUtils;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.models.AppType;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.v1.Permission;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.service.handler.AdminServiceHandler;
import io.isima.bios.service.handler.FanRouter;
import io.isima.bios.utils.AdminConfigCreator;
import io.isima.bios.utils.BiosObjectMapperProvider;
import io.isima.bios.utils.StringUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class AdminTestUtils {

  private static final ObjectMapper mapper = BiosObjectMapperProvider.get();

  public static String populateContext(
      io.isima.bios.admin.Admin admin, String tenantName, String jsonFile) throws Exception {
    final ContextConfig conf = loadJson(jsonFile, ContextConfig.class);
    return populateContext(admin, tenantName, conf);
  }

  public static String populateContext(
      io.isima.bios.admin.Admin admin, String tenantName, ContextConfig conf) throws Exception {
    long timestamp = System.currentTimeMillis();
    if (conf.getAuditEnabled() == Boolean.TRUE) {
      final var auditSignal =
          ContextAudit.makeConfigFundamentalPart(
              conf, StringUtils.prefixToCamelCase(CONTEXT_AUDIT_SIGNAL_PREFIX, conf.getName()));
      admin.createSignalAllowingInternalAttributes(
          tenantName, auditSignal, RequestPhase.INITIAL, timestamp, Set.of("_operation"));
      admin.createSignalAllowingInternalAttributes(
          tenantName, auditSignal, RequestPhase.FINAL, timestamp, Set.of("_operation"));
    }
    admin.createContext(tenantName, conf, RequestPhase.INITIAL, ++timestamp);
    admin.createContext(tenantName, conf, RequestPhase.FINAL, timestamp);
    return conf.getName();
  }

  public static String populateSignal(
      io.isima.bios.admin.Admin admin, String tenantName, String jsonFile) throws Exception {
    long timestamp = System.currentTimeMillis();
    final SignalConfig conf = loadJson(jsonFile, SignalConfig.class);
    admin.createSignal(tenantName, conf, RequestPhase.INITIAL, timestamp);
    admin.createSignal(tenantName, conf, RequestPhase.FINAL, timestamp);
    return conf.getName();
  }

  public static void updateContext(
      io.isima.bios.admin.Admin admin, String tenantName, String jsonFile) throws Exception {
    long timestamp = System.currentTimeMillis();
    final ContextConfig conf = loadJson(jsonFile, ContextConfig.class);
    admin.updateContext(
        tenantName, conf.getName(), conf, RequestPhase.INITIAL, timestamp, Set.of());
    admin.updateContext(tenantName, conf.getName(), conf, RequestPhase.FINAL, timestamp, Set.of());
  }

  public static void updateSignal(
      io.isima.bios.admin.Admin admin, String tenantName, String jsonFile) throws Exception {
    long timestamp = System.currentTimeMillis();
    final SignalConfig conf = loadJson(jsonFile, SignalConfig.class);
    admin.updateSignal(tenantName, conf.getName(), conf, RequestPhase.INITIAL, timestamp);
    admin.updateSignal(tenantName, conf.getName(), conf, RequestPhase.FINAL, timestamp);
  }

  public static StreamConfig makeStreamConfig(String src, Long timestamp) throws Exception {
    final StreamConfig conf = mapper.readValue(src.replaceAll("'", "\""), StreamConfig.class);
    conf.setVersion(timestamp);
    return conf;
  }

  public static StreamConfig getStreamConfig(String jsonFile) throws Exception {
    final String streamJson = new String(Files.readAllBytes(Paths.get(jsonFile)));
    final StreamConfig conf = mapper.readValue(streamJson, StreamConfig.class);
    conf.setVersion(System.currentTimeMillis());
    return conf;
  }

  public static void addTenant(AdminInternal admin, String tenantName) throws Exception {
    TenantConfig tenantConfig = new TenantConfig(tenantName).setVersion(System.currentTimeMillis());
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, tenantConfig.getVersion());
    admin.addTenant(tenantConfig, RequestPhase.FINAL, tenantConfig.getVersion());
  }

  public static void deleteTenantIgnoreError(AdminInternal admin, String tenantName) {
    try {
      TenantConfig tenantConfig =
          new TenantConfig(tenantName).setVersion(System.currentTimeMillis());
      admin.removeTenant(tenantConfig, RequestPhase.INITIAL, tenantConfig.getVersion());
      admin.removeTenant(tenantConfig, RequestPhase.FINAL, tenantConfig.getVersion());
    } catch (Exception e) {
      // Ignore it.
    }
  }

  public static <T> T loadJson(String filePath, Class<T> clazz) throws IOException {
    final String src = new String(Files.readAllBytes(Paths.get(filePath)));
    return mapper.readValue(src, clazz);
  }

  public static void populateStream(
      AdminInternal admin, String tenantName, String src, Long timestamp) throws Exception {
    final StreamConfig conf = AdminConfigCreator.makeStreamConfig(src, timestamp);
    admin.addStream(tenantName, conf, RequestPhase.INITIAL);
    admin.addStream(tenantName, conf, RequestPhase.FINAL);
  }

  /** Creates a stream using tfos JSON schema from a resource file and returns the stream's name. */
  public static String populateStream(AdminInternal admin, String tenantName, String jsonFile)
      throws Exception {
    final Path path = Paths.get(jsonFile);
    final String streamJson = new String(Files.readAllBytes(path));
    final StreamConfig conf =
        AdminConfigCreator.makeStreamConfig(streamJson, System.currentTimeMillis());
    admin.addStream(tenantName, conf, RequestPhase.INITIAL);
    admin.addStream(tenantName, conf, RequestPhase.FINAL);
    return conf.getName();
  }

  /** Creates a stream using tfos JSON schema from a resource file. */
  public static String populateStream(
      AdminInternal admin, String tenantName, String streamName, String jsonFile) throws Exception {
    final Path path = Paths.get(jsonFile);
    final String streamJson = new String(Files.readAllBytes(path));
    final StreamConfig conf =
        AdminConfigCreator.makeStreamConfig(streamJson, System.currentTimeMillis());
    conf.setName(streamName);
    admin.addStream(tenantName, conf, RequestPhase.INITIAL);
    admin.addStream(tenantName, conf, RequestPhase.FINAL);
    return conf.getName();
  }

  /** Creates a stream using stream config object and returns the stream descriptor. */
  public static StreamDesc populateStream(AdminInternal admin, String tenantName, StreamConfig conf)
      throws Exception {
    conf.setVersion(System.currentTimeMillis());
    admin.addStream(tenantName, conf, RequestPhase.INITIAL);
    admin.addStream(tenantName, conf, RequestPhase.FINAL);
    return admin.getStream(tenantName, conf.getName());
  }

  public static void deleteStream(AdminInternal admin, String tenantName, String streamName)
      throws Exception {
    final var timestamp = System.currentTimeMillis();
    admin.removeStream(tenantName, streamName, RequestPhase.INITIAL, timestamp);
    admin.removeStream(tenantName, streamName, RequestPhase.FINAL, timestamp);
    return;
  }

  public static void deleteStreamIgnorePresence(
      AdminInternal admin, String tenantName, String streamName) throws Exception {
    try {
      deleteStream(admin, tenantName, streamName);
    } catch (NoSuchStreamException e) {
      // This is OK.
    }
  }

  public static void setupTenant(
      io.isima.bios.admin.Admin admin, String tenantName, boolean recreate)
      throws ApplicationException, TfosException {
    final var tenantConfig = new io.isima.bios.models.TenantConfig(tenantName);
    if (recreate) {
      tearDownTenant(admin, tenantName);
    }
    final long timestamp = System.currentTimeMillis();
    admin.createTenant(tenantConfig, RequestPhase.INITIAL, timestamp, false);
    admin.createTenant(tenantConfig, RequestPhase.FINAL, timestamp, false);
  }

  public static void tearDownTenant(io.isima.bios.admin.Admin admin, String tenantName)
      throws ApplicationException, TfosException {
    long timestamp = System.currentTimeMillis();
    try {
      admin.deleteTenant(tenantName, RequestPhase.INITIAL, timestamp);
      admin.deleteTenant(tenantName, RequestPhase.FINAL, timestamp);
    } catch (NoSuchTenantException e) {
      // ignore
    }
  }

  public static void createSignal(
      Auth auth, AdminServiceHandler adminHandler, String tenantName, SignalConfig signalConfig)
      throws ApplicationException, TfosException {
    final var sessionToken =
        TestUtils.makeTestSessionToken(
            auth, tenantName, List.of(Permission.ADMIN), AppType.ADHOC, "test");
    final FanRouter<SignalConfig, SignalConfig> fanRouter = TestUtils.makeDummyFanRouter();
    long timestamp = System.currentTimeMillis();
    DataUtils.wait(
        adminHandler.createSignal(
            sessionToken,
            tenantName,
            signalConfig,
            RequestPhase.INITIAL,
            timestamp,
            Optional.of(fanRouter),
            TestUtils.makeGenericState()),
        "create signal");
  }

  public static void createSignal(
      Auth auth, AdminServiceHandler adminHandler, String tenantName, String signalConfigSrc)
      throws ApplicationException, TfosException, JsonProcessingException {
    final var signalConfig = mapper.readValue(signalConfigSrc, SignalConfig.class);
    createSignal(auth, adminHandler, tenantName, signalConfig);
  }

  public static ContextConfig createContext(
      Auth auth, AdminServiceHandler adminHandler, String tenantName, ContextConfig contextConfig)
      throws ApplicationException, TfosException {
    final var sessionToken =
        TestUtils.makeTestSessionToken(
            auth, tenantName, List.of(Permission.ADMIN), AppType.ADHOC, "test");
    final FanRouter<ContextConfig, ContextConfig> contextFanRouter = TestUtils.makeDummyFanRouter();
    final FanRouter<SignalConfig, SignalConfig> signalFanRouter = TestUtils.makeDummyFanRouter();
    long timestamp = System.currentTimeMillis();
    final var auditSignalName = StringUtils.prefixToCamelCase("audit", contextConfig.getName());
    return DataUtils.wait(
        adminHandler.createContext(
            sessionToken,
            tenantName,
            contextConfig,
            RequestPhase.INITIAL,
            timestamp,
            Optional.of(contextFanRouter),
            auditSignalName,
            Optional.of(signalFanRouter),
            TestUtils.makeGenericState()),
        "create context");
  }

  public static ContextConfig createContext(
      Auth auth, AdminServiceHandler adminHandler, String tenantName, String contextConfigSrc)
      throws ApplicationException, TfosException, JsonProcessingException {
    final var contextConfig =
        mapper.readValue(contextConfigSrc.replace("'", "\""), ContextConfig.class);
    return createContext(auth, adminHandler, tenantName, contextConfig);
  }

  public static SignalConfig updateSignal(
      Auth auth,
      AdminServiceHandler adminHandler,
      String tenantName,
      String signalName,
      SignalConfig signalConfig)
      throws ApplicationException, TfosException {
    final var sessionToken =
        TestUtils.makeTestSessionToken(
            auth, tenantName, List.of(Permission.ADMIN), AppType.ADHOC, "test");
    final FanRouter<ContextConfig, ContextConfig> contextFanRouter = TestUtils.makeDummyFanRouter();
    final FanRouter<SignalConfig, SignalConfig> signalFanRouter = TestUtils.makeDummyFanRouter();
    long timestamp = System.currentTimeMillis();
    return DataUtils.wait(
        adminHandler.updateSignal(
            sessionToken,
            tenantName,
            signalName,
            signalConfig,
            RequestPhase.INITIAL,
            timestamp,
            Optional.of(signalFanRouter),
            true,
            TestUtils.makeGenericState()),
        "update signal");
  }

  public static ContextConfig updateContext(
      Auth auth,
      AdminServiceHandler adminHandler,
      String tenantName,
      String contextName,
      ContextConfig contextConfig)
      throws ApplicationException, TfosException {
    final var sessionToken =
        TestUtils.makeTestSessionToken(
            auth, tenantName, List.of(Permission.ADMIN), AppType.ADHOC, "test");
    final FanRouter<ContextConfig, ContextConfig> contextFanRouter = TestUtils.makeDummyFanRouter();
    final FanRouter<SignalConfig, SignalConfig> signalFanRouter = TestUtils.makeDummyFanRouter();
    long timestamp = System.currentTimeMillis();
    final var auditSignalName = StringUtils.prefixToCamelCase("audit", contextConfig.getName());
    return DataUtils.wait(
        adminHandler.updateContext(
            sessionToken,
            tenantName,
            contextName,
            contextConfig,
            RequestPhase.INITIAL,
            timestamp,
            Optional.of(contextFanRouter),
            auditSignalName,
            Optional.of(signalFanRouter),
            Optional.of(signalFanRouter),
            TestUtils.makeGenericState()),
        "update context");
  }
}
