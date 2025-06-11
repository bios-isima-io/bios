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

/** Unit Test for SharedConfig class. */
package io.isima.bios.admin.v1;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.partialMockBuilder;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.isima.bios.admin.v1.impl.SharedPropertiesImpl;
import io.isima.bios.common.SharedProperties;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.models.LbPolicy;
import io.isima.bios.models.OperationPolicy;
import io.isima.bios.models.OperationSet;
import io.isima.bios.models.OperationType;
import io.isima.bios.models.Upstream;
import io.isima.bios.models.UpstreamConfig;
import io.isima.bios.models.UpstreamOperation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Unit test for SharedConfig Class. This UT was created as part of CSDK-Loadbalancing effort hence
 * it does not cover the code that was written before CSDK loadbalancing feature.
 */
public class SharedConfigTest {

  /**
   * Generated form eclipse.
   *
   * @throws java.lang.Exception in some cases
   */
  private SharedConfig sharedConfig;

  private static SharedProperties properties;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {}

  /**
   * Generated function.
   *
   * @throws java.lang.Exception under some conditions
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  /**
   * Generated from eclipse.
   *
   * @throws java.lang.Exception in some cases
   */
  @Before
  public void setUp() throws Exception {}

  /**
   * Generated from eclipse.
   *
   * @throws java.lang.Exception under some conditions
   */
  @After
  public void tearDown() throws Exception {}

  /**
   * This testcase tests if the upstream string is returned from the shared properties, then it is
   * properly converted into UpstreamConfig object.
   *
   * @throws ApplicationException when upstream config json can't be parsed
   */
  @Test
  public void testGetUpstreamFromSharedProperties() throws ApplicationException {

    String upstreamConfigJson =
        "{"
            + "  \"lbPolicy\": \"ROUND_ROBIN\","
            + "  \"epoch\": \"0\","
            + "  \"Version\": \"0\","
            + "  \"upstreams\": ["
            + "    {"
            + "      \"hostSet\": ["
            + "        \"bios-signal-1\""
            + "      ],"
            + "      \"operationSet\": {"
            + "        \"operation\": ["
            + "          {"
            + "            \"operationPolicy\": \"ALWAYS\","
            + "            \"operationType\": \"INGEST\""
            + "          },"
            + "          {"
            + "            \"operationPolicy\": \"ALWAYS\","
            + "            \"operationType\": \"CONTEXT_WRITE\""
            + "          }"
            + "        ]"
            + "      }"
            + "    },"
            + "    {"
            + "      \"hostSet\": ["
            + "        \"bios-analysis-1\""
            + "      ],"
            + "      \"operationSet\": {"
            + "        \"operation\": ["
            + "          {"
            + "            \"operationPolicy\": \"UNDERFAILURE\","
            + "            \"operationType\": \"INGEST\""
            + "          },"
            + "          {"
            + "            \"operationPolicy\": \"NEVER\","
            + "            \"operationType\": \"EXTRACT\""
            + "          },"
            + "          {"
            + "            \"operationPolicy\": \"ALWAYS\","
            + "            \"operationType\": \"ADMIN_READ\""
            + "          },"
            + "          {"
            + "            \"operationPolicy\": \"ALWAYS\","
            + "            \"operationType\": \"ADMIN_WRITE\""
            + "          },"
            + "          {"
            + "            \"operationPolicy\": \"ALWAYS\","
            + "            \"operationType\": \"SUMMARIZE\""
            + "          }"
            + "        ]"
            + "      }"
            + "    },"
            + "    {"
            + "      \"hostSet\": ["
            + "        \"bios-rollup-1\""
            + "      ],"
            + "      \"operationSet\": {"
            + "        \"operation\": ["
            + "          {"
            + "            \"operationPolicy\": \"UNDERFAILURE\","
            + "            \"operationType\": \"INGEST\""
            + "          },"
            + "          {"
            + "            \"operationPolicy\": \"ALWAYS\","
            + "            \"operationType\": \"EXTRACT\""
            + "          }"
            + "        ]"
            + "      }"
            + "    }"
            + "  ]"
            + "}";
    properties =
        partialMockBuilder(SharedPropertiesImpl.class).addMockedMethods("getProperty").createMock();
    expect(properties.getProperty(SharedConfig.UPSTREAM)).andReturn(upstreamConfigJson).anyTimes();
    replay(properties);

    sharedConfig = new SharedConfig(properties);
    UpstreamConfig upstreamConfig;

    upstreamConfig = sharedConfig.getUpstreamConfig();

    assertNotNull(upstreamConfig);
    assertNotNull(upstreamConfig.getUpstreams());
    assertEquals(upstreamConfig.getUpstreams().size(), 3);
    assertEquals(upstreamConfig.getLbPolicy(), LbPolicy.ROUND_ROBIN);
    assertEquals(upstreamConfig.getEpoch(), 0);
    assertEquals(upstreamConfig.getVersion(), 0);
    assertTrue(upstreamConfig.toString().contains("lbPolicy = ROUND_ROBIN]"));
    assertNotNull(upstreamConfig.getUpstreams().get(0));
    Upstream upstream;
    upstream = upstreamConfig.getUpstreams().get(0);
    assertNotNull(upstream.getHostSet());
    assertNotNull(upstream.getOperationSet());
    assertNotNull(upstream.getHostSet()[0]);
    assertEquals(upstream.getHostSet()[0], "bios-signal-1");
    OperationSet opSet;
    opSet = upstream.getOperationSet();
    assertNotNull(opSet.getOperation());
    assertNotNull(opSet.getOperation().get(0));
    assertEquals(opSet.getOperation().get(0).getOperationType(), OperationType.INGEST);
    assertEquals(opSet.getOperation().get(0).getOperationPolicy(), OperationPolicy.ALWAYS);
    assertEquals(opSet.getOperation().get(1).getOperationType(), OperationType.CONTEXT_WRITE);
    assertEquals(opSet.getOperation().get(1).getOperationPolicy(), OperationPolicy.ALWAYS);
  }

  /**
   * This is a negative test case. It tests that if there is an unknown field in the json, then the
   * conversion should throw exception. This will be enabled once BB-731 is fixed.
   *
   * @throws ApplicationException when upstream config json cant be parsed
   */
  @Ignore
  @Test(expected = ApplicationException.class)
  public void testGetUpstreamWrongField() throws ApplicationException {
    // It should have been upstreams instead of Upstreams
    String upstreamConfigJson =
        "{\n"
            + "  \"lbPolicy\": \"ROUND_ROBIN\"," // This is wrong
            + "  \"uptream\": ["
            + "    {"
            + "      \"hostSet\": ["
            + "        \"bios-signal-1\""
            + "      ],"
            + "      \"operationSet\": {"
            + "        \"operation\": ["
            + "          {"
            + "            \"operationPolicy\": \"ALWAYS\","
            + "            \"operationType\": \"INGEST\""
            + "          },"
            + "          {"
            + "            \"operationPolicy\": \"ALWAYS\","
            + "            \"operationType\": \"CONTEXT_WRITE\""
            + "          }"
            + "        ]"
            + "      }"
            + "    }"
            + "  ]"
            + "}";
    properties =
        partialMockBuilder(SharedPropertiesImpl.class).addMockedMethods("getProperty").createMock();
    expect(properties.getProperty(SharedConfig.UPSTREAM)).andReturn(upstreamConfigJson).anyTimes();
    expect(properties.getProperty(SharedConfig.NODES)).andReturn("").anyTimes();
    replay(properties);

    sharedConfig = new SharedConfig(properties);
    UpstreamConfig upstreamConfig = sharedConfig.getUpstreamConfig();
    assertNull(upstreamConfig);
  }

  /**
   * This is a negative test case. It tests that if shared properties returns a malformed json, then
   * the shared config raises ApplicationException
   *
   * @throws ApplicationException when upstream config json cant be parsed
   */
  @Test(expected = ApplicationException.class)
  public void testGetUpstreamWrongJsonSyntax() throws ApplicationException {
    String upstreamConfigJson = "{";
    properties =
        partialMockBuilder(SharedPropertiesImpl.class).addMockedMethods("getProperty").createMock();
    expect(properties.getProperty(SharedConfig.UPSTREAM)).andReturn(upstreamConfigJson).anyTimes();
    expect(properties.getProperty(SharedConfig.NODES)).andReturn("").anyTimes();
    replay(properties);

    sharedConfig = new SharedConfig(properties);
    UpstreamConfig upstreamConfig = sharedConfig.getUpstreamConfig();
    assertNull(upstreamConfig);
  }

  /**
   * This is a negative test case and it tests that if a empty json is returned from shared config,
   * then the upstream config object is null.
   *
   * @throws ApplicationException when upstream config json cant be parsed
   */
  @Test
  public void testGetUpstreamEmptyJson() throws ApplicationException {
    properties =
        partialMockBuilder(SharedPropertiesImpl.class).addMockedMethods("getProperty").createMock();
    expect(properties.getProperty(SharedConfig.UPSTREAM)).andReturn("").anyTimes();
    expect(properties.getProperty(SharedConfig.NODES)).andReturn("").anyTimes();
    replay(properties);

    sharedConfig = new SharedConfig(properties);
    UpstreamConfig upstreamConfig = sharedConfig.getUpstreamConfig();
    assertNull(upstreamConfig);
  }

  /**
   * This is a negative test case. It tests that there is no upstream returned if there are no
   * endpoints configured and the endpoints returns null object.
   *
   * @throws ApplicationException when upstream config json cant be parsed
   */
  @Test
  public void testGetUpstreamNoEndpoints() throws ApplicationException {
    properties =
        partialMockBuilder(SharedPropertiesImpl.class).addMockedMethods("getProperty").createMock();
    expect(properties.getProperty(SharedConfig.UPSTREAM)).andReturn(null).anyTimes();
    expect(properties.getProperty(SharedConfig.NODES)).andReturn(null).anyTimes();
    replay(properties);

    sharedConfig = new SharedConfig(properties);
    UpstreamConfig upstreamConfig = sharedConfig.getUpstreamConfig();
    assertNull(upstreamConfig);
  }

  /**
   * This is a negative test case. It tests that there is no upstream returned if there are no
   * endpoints configured and the endpoints returns empty string.
   *
   * @throws ApplicationException when upstream config json cant be parsed
   */
  @Test
  public void testGetUpstreamEmptyEndpoints() throws ApplicationException {
    properties =
        partialMockBuilder(SharedPropertiesImpl.class).addMockedMethods("getProperty").createMock();
    expect(properties.getProperty(SharedConfig.UPSTREAM)).andReturn(null).anyTimes();
    expect(properties.getProperty(SharedConfig.NODES)).andReturn("").anyTimes();
    replay(properties);

    sharedConfig = new SharedConfig(properties);
    UpstreamConfig upstreamConfig = sharedConfig.getUpstreamConfig();
    assertNull(upstreamConfig);
  }

  /**
   * This test creates upstream config from endpoints. SharedProperties is mocked to return 3
   * endpoints, one each of signal, analysis and rollup. Then we tets if everything is as expected.
   *
   * @throws ApplicationException when upstream config json cant be parsed
   */
  @Test
  public void testGetUpstreamFromSignalEndpoint() throws ApplicationException {
    String nodes = "signal:172.18.0.1";

    properties =
        partialMockBuilder(SharedPropertiesImpl.class).addMockedMethods("getProperty").createMock();
    expect(properties.getProperty(SharedConfig.UPSTREAM)).andReturn(null).anyTimes();
    expect(properties.getProperty(SharedConfig.NODES)).andReturn(nodes).anyTimes();
    replay(properties);

    sharedConfig = new SharedConfig(properties);
    UpstreamConfig upstreamConfig;
    upstreamConfig = sharedConfig.getUpstreamConfig();
    assertNotNull(upstreamConfig);
    assertNotNull(upstreamConfig.getUpstreams());
    assertEquals(upstreamConfig.getUpstreams().size(), 1);
    assertEquals(upstreamConfig.getLbPolicy(), LbPolicy.ROUND_ROBIN);
    assertTrue(upstreamConfig.toString().contains("lbPolicy = ROUND_ROBIN]"));
    assertNotNull(upstreamConfig.getUpstreams().get(0));
    Upstream upstream;
    upstream = upstreamConfig.getUpstreams().get(0);
    assertNotNull(upstream.getHostSet());
    assertNotNull(upstream.getOperationSet());
    assertNotNull(upstream.getHostSet()[0]);
    assertTrue(upstream.toString().contains("hostSet"));
    assertEquals(upstream.getHostSet()[0], "172.18.0.1");
    OperationSet opSet;
    opSet = upstream.getOperationSet();
    assertNotNull(opSet.getOperation());
    assertTrue(opSet.toString().contains("Operation"));
    assertNotNull(opSet.getOperation().get(0));
    UpstreamOperation op;
    op = opSet.getOperation().get(0);
    assertTrue(op.toString().contains("Policy"));
    assertTrue(opSet.getOperation().toString().contains("Operation"));
    assertEquals(opSet.getOperation().get(0).getOperationType(), OperationType.INGEST);
    assertEquals(opSet.getOperation().get(0).getOperationPolicy(), OperationPolicy.ALWAYS);
    assertEquals(opSet.getOperation().get(1).getOperationType(), OperationType.CONTEXT_WRITE);
    assertEquals(opSet.getOperation().get(1).getOperationPolicy(), OperationPolicy.ALWAYS);
  }

  @Test
  public void testGetUpstreamFromAllEndpoints() throws ApplicationException {
    String nodes = "signal:172.18.0.1\n" + "analysis:172.18.0.2\n" + "rollup:172.18.0.3";

    properties =
        partialMockBuilder(SharedPropertiesImpl.class).addMockedMethods("getProperty").createMock();
    expect(properties.getProperty(SharedConfig.UPSTREAM)).andReturn(null).anyTimes();
    expect(properties.getProperty(SharedConfig.NODES)).andReturn(nodes).anyTimes();
    replay(properties);

    sharedConfig = new SharedConfig(properties);
    UpstreamConfig upstreamConfig;
    upstreamConfig = sharedConfig.getUpstreamConfig();
    assertNotNull(upstreamConfig);
    assertNotNull(upstreamConfig.getUpstreams());
    assertEquals(upstreamConfig.getUpstreams().size(), 3);
    assertEquals(upstreamConfig.getLbPolicy(), LbPolicy.ROUND_ROBIN);

    assertNotNull(upstreamConfig.getUpstreams().get(0));
    Upstream upstream;
    upstream = upstreamConfig.getUpstreams().get(0);
    assertNotNull(upstream.getHostSet());
    assertNotNull(upstream.getOperationSet());
    assertNotNull(upstream.getHostSet()[0]);
    assertEquals(upstream.getHostSet()[0], "172.18.0.1");
    OperationSet opSet;
    opSet = upstream.getOperationSet();
    assertNotNull(opSet.getOperation());
    assertNotNull(opSet.getOperation().get(0));
    assertEquals(opSet.getOperation().get(0).getOperationType(), OperationType.INGEST);
    assertEquals(opSet.getOperation().get(0).getOperationPolicy(), OperationPolicy.ALWAYS);
    assertEquals(opSet.getOperation().get(1).getOperationType(), OperationType.CONTEXT_WRITE);
    assertEquals(opSet.getOperation().get(1).getOperationPolicy(), OperationPolicy.ALWAYS);

    assertNotNull(upstreamConfig.getUpstreams().get(1));
    upstream = upstreamConfig.getUpstreams().get(1);
    assertNotNull(upstream.getHostSet());
    assertNotNull(upstream.getOperationSet());
    assertNotNull(upstream.getHostSet()[0]);
    assertEquals(upstream.getHostSet()[0], "172.18.0.2");
    opSet = upstream.getOperationSet();
    assertNotNull(opSet.getOperation());
    assertNotNull(opSet.getOperation().get(0));
    assertEquals(opSet.getOperation().get(0).getOperationType(), OperationType.SUMMARIZE);
    assertEquals(opSet.getOperation().get(0).getOperationPolicy(), OperationPolicy.ALWAYS);
    assertEquals(opSet.getOperation().get(1).getOperationType(), OperationType.ADMIN_READ);
    assertEquals(opSet.getOperation().get(1).getOperationPolicy(), OperationPolicy.ALWAYS);
    assertEquals(opSet.getOperation().get(2).getOperationType(), OperationType.ADMIN_WRITE);
    assertEquals(opSet.getOperation().get(2).getOperationPolicy(), OperationPolicy.ALWAYS);

    assertNotNull(upstreamConfig.getUpstreams().get(2));
    upstream = upstreamConfig.getUpstreams().get(2);
    assertNotNull(upstream.getHostSet());
    assertNotNull(upstream.getOperationSet());
    assertNotNull(upstream.getHostSet()[0]);
    assertEquals(upstream.getHostSet()[0], "172.18.0.3");
    opSet = upstream.getOperationSet();
    assertNotNull(opSet.getOperation());
    assertNotNull(opSet.getOperation().get(0));
    assertEquals(opSet.getOperation().get(0).getOperationType(), OperationType.EXTRACT);
    assertEquals(opSet.getOperation().get(0).getOperationPolicy(), OperationPolicy.ALWAYS);
  }
}
