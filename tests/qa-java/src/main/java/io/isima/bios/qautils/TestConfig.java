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
package io.isima.bios.qautils;

import java.net.URI;
import java.net.URISyntaxException;

public final class TestConfig {
  public static final String admin_user = "admin";
  public static final String admin_pass = "admin";
  public static final String sadmin_user = "superadmin";
  public static final String sadmin_pass = "superadmin";
  public static final String ingest_user = "ingest";
  public static final String ingest_pass = "ingest";
  public static final String extract_user = "extract";
  public static final String extract_pass = "extract";

  private static int port = -1;

  private static final int DEFAULT_SECURE_PORT = 443;
  private static final int DEFAULT_INSECURE_PORT = 80;

  /**
   * Method to get a single-node TFOS service endpoint.
   *
   * <p>The method returns endpoint 'localhost' in default.
   *
   * <p>The value can be changed by environment variable TFOS_JAVA_ENDPOINT.
   */
  public static String getEndpoint() {
    final var endpointUrl = getBiosEndpoint();
    if (endpointUrl != null) {
      return endpointUrl.getHost();
    }
    String hostName = System.getenv("TFOS_JAVA_ENDPOINT");
    if (hostName != null) {
      return hostName;
    }
    return "localhost";
  }

  /**
   * Method to get port number of TFOS endpoint.
   *
   * <p>If environment variable TFOS_JAVA_ENDPOINT_PORT is set, the value is used. Otherwise,
   * default port number 443 is used for secured connection; 80 for insecured one.
   *
   * @return TFOS service endpoint port
   */
  public static int getPort() {
    return getPortNumber(getSslEnabled());
  }

  /**
   * Method to get port number of TFOS endpoint.
   *
   * <p>If environment variable TFOS_JAVA_ENDPOINT_PORT is set, the value is used. Otherwise,
   * default port number 443 is used for secured connection; 80 for insecured one.
   *
   * @param isSecure Flag to use secure connection. This parameter is ignored when
   *     TFOS_JAVA_ENDPOINT_PORT is set.
   * @return TFOS service endpoint port
   */
  private static int getPortNumber(boolean isSecure) {
    if (port < 0) {
      final var endpointUrl = getBiosEndpoint();
      if (endpointUrl != null) {
        final int specifiedPort = endpointUrl.getPort();
        port =
            specifiedPort > 0 ? specifiedPort : "https".equals(endpointUrl.getScheme()) ? 443 : 80;
      } else {
        final String strPort = System.getenv("TFOS_JAVA_ENDPOINT_PORT");
        if (strPort == null) {
          port = isSecure ? DEFAULT_SECURE_PORT : DEFAULT_INSECURE_PORT;
        } else {
          port = Integer.parseInt(strPort);
        }
      }
    }
    return port;
  }

  /**
   * Method to fetch TFOS_JAVA_ENPOINT_FLAG.
   *
   * <p>When this environmental variable is not set. Default flag is 'True'. if true https else http
   */
  public static boolean getSslEnabled() {
    final var endpointUrl = getBiosEndpoint();
    if (endpointUrl != null) {
      return "https".equalsIgnoreCase(endpointUrl.getScheme());
    }
    final String isSecured = System.getenv("TFOS_JAVA_ENDPOINT_FLAG");
    final boolean flag = (isSecured == null) ? true : Boolean.parseBoolean(isSecured);
    return flag;
  }

  private static URI getBiosEndpoint() {
    String endpointUrlSrc = System.getenv("BIOS_ENDPOINT");
    if (endpointUrlSrc == null) {
      endpointUrlSrc = System.getenv("TFOS_ENDPOINT");
    }
    if (endpointUrlSrc != null) {
      try {
        return new URI(endpointUrlSrc);
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  /**
   * Method to fetch TFOS_JAVA_AUTH_DISABLE.
   *
   * <p>When this environmental variable is not set. Default flag is 'false'.
   *
   * @return TFOS_JAVA_AUTH_DISABLE flag
   */
  public static boolean getAuthDisabled() {

    String strAuthDisable = System.getenv("TFOS_JAVA_AUTH_DISABLE");
    boolean authDisable = Boolean.parseBoolean(strAuthDisable);
    if (strAuthDisable == null) {
      authDisable = false;
      return authDisable;
    }
    return authDisable;
  }

  /**
   * Method to fetch SSL cert file path.
   *
   * <p>Environment variable SSL_CERT_FILE overrides the return value. If it is not set, the default
   * certificate file path is returned.
   *
   * @return TFOS service certificate file path
   */
  public static String getCertPath() {
    String certPath = System.getenv("SSL_CERT_FILE");
    if (certPath == null) {
      certPath = "../../../../../../../../../test_data/cert.pem";
      return certPath;
    }
    return certPath;
  }

  /**
   * Returns whether the test is conigured as integration test.
   *
   * <p>The test framework, i.e. pom.xml files, sets system property skipIT=true when the test is
   * NOT executed as integrateion test.ssssss
   *
   * @return
   */
  public static boolean isIntegrationTest() {
    return !Boolean.parseBoolean(System.getProperty("skipIT", "false"));
  }

  /**
   * Returns rollup interval seconds configured by environment variable ROLLUP_INTERVAL.
   *
   * @return Rollup interval set by environment variable ROLLUP_INTERVAL. The default value is 300.
   *     If the configured value is zero or less than zero, the returning value would be 1.
   * @throws NumberFormatException when the environment variable ROLLUP_INTERVAL has invalid syntax.
   */
  public static int getRollupIntervalSeconds() {
    String endpoint = System.getenv("ROLLUP_INTERVAL");
    final int defaultValue = 300;
    if (endpoint == null) {
      return defaultValue;
    }
    int value = Integer.parseInt(endpoint);
    return Math.max(value, 1);
  }
}
