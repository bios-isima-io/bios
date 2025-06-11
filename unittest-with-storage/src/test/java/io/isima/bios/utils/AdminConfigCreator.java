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
package io.isima.bios.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.TenantConfig;

public class AdminConfigCreator {
  private static final ObjectMapper mapper = TfosObjectMapperProvider.get();

  /**
   * Makes a tenant config object from a JSON-like source string.
   *
   * <p>The source string should be similar to JSON but double quotes have to be replaced by single
   * quotes. For example,
   *
   * <pre>
   * {'name':'my_tenant'}
   * </pre>
   *
   * @param src Source JSON-like string
   * @param version Tenant version to put. This parameter replaces version in the source string if
   *     there is.
   * @return Created TenantConfig object
   * @throws Exception When the input string is invalid
   * @throws NullPointerException when src or version is null
   */
  public static TenantConfig makeTenantConfig(String src, Long version) throws Exception {
    if (src == null || version == null) {
      throw new NullPointerException("parameters must not be null");
    }
    return makeTenantConfigCore(src, version);
  }

  /**
   * Makes a tenant config object from a JSON-like source string.
   *
   * <p>The source string should be similar to JSON but double quotes have to be replaced by single
   * quotes. For example,
   *
   * <pre>
   * {'name':'my_tenant'}
   * </pre>
   *
   * @param src Source JSON-like string
   * @return Created TenantConfig object
   * @throws Exception When the input string is invalid
   * @throws NullPointerException when src is null
   */
  public static TenantConfig makeTenantConfig(String src) throws Exception {
    if (src == null) {
      throw new NullPointerException("parameters must not be null");
    }
    return makeTenantConfigCore(src, null);
  }

  private static TenantConfig makeTenantConfigCore(String src, Long version) throws Exception {
    final TenantConfig conf = mapper.readValue(src.replaceAll("'", "\""), TenantConfig.class);
    if (version != null) {
      conf.setVersion(version);
    }
    return conf;
  }

  /**
   * Makes a stream config object from a JSON-like source string.
   *
   * <p>The source string should be similar to JSON but double quotes have to be replaced by single
   * quotes. For example,
   *
   * <pre>
   * {'name':'my_stream','attributes':[{'name':'hello','type':'string'}]}
   * </pre>
   *
   * @param src Source JSON-like string
   * @param version Stream version to put. This parameter replaces version in the source string if
   *     there is.
   * @return Created StreamConfig object
   * @throws Exception When the input string is invalid
   * @throws NullPointerException when src or version is null
   */
  public static StreamConfig makeStreamConfig(String src, Long version) throws Exception {
    if (src == null) {
      throw new NullPointerException("parameters must not be null");
    }
    return makeStreamConfigCore(src, version);
  }

  /**
   * Makes a stream config object from a JSON-like source string.
   *
   * <p>The source string should be similar to JSON but double quotes have to be replaced by single
   * quotes. For example,
   *
   * <pre>
   * {'name':'my_stream','attributes':[{'name':'hello','type':'string'}]}
   * </pre>
   *
   * @param src Source JSON-like string
   * @return Created StreamConfig object
   * @throws Exception When the input string is invalid
   * @throws NullPointerException when src or version is null
   */
  public static StreamConfig makeStreamConfig(String src) throws Exception {
    if (src == null) {
      throw new NullPointerException("parameters must not be null");
    }
    return makeStreamConfigCore(src, null);
  }

  private static StreamConfig makeStreamConfigCore(String src, Long version) throws Exception {
    final StreamConfig conf = mapper.readValue(src.replaceAll("'", "\""), StreamConfig.class);
    if (version != null) {
      conf.setVersion(version);
    }
    return conf;
  }
}
