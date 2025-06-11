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
package io.isima.bios.admin.v1.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.nio.file.Files;
import java.nio.file.Paths;

public class AdminTestUtils {

  private static ObjectMapper mapper = TfosObjectMapperProvider.get();

  public static void populateStream(
      AdminInternal admin, String tenantName, String src, Long timestamp) throws Exception {
    final StreamConfig conf = AdminTestUtils.makeStreamConfig(src, timestamp);
    admin.addStream(tenantName, conf, RequestPhase.INITIAL);
    admin.addStream(tenantName, conf, RequestPhase.FINAL);
  }

  /** Creates a stream using tfos JSON schema from a resource file and returns the stream's name. */
  public static String populateStream(AdminInternal admin, String tenantName, String jsonFile)
      throws Exception {
    final String streamJson = new String(Files.readAllBytes(Paths.get(jsonFile)));
    final StreamConfig conf = mapper.readValue(streamJson, StreamConfig.class);
    conf.setVersion(System.currentTimeMillis());
    admin.addStream(tenantName, conf, RequestPhase.INITIAL);
    admin.addStream(tenantName, conf, RequestPhase.FINAL);
    return conf.getName();
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
}
