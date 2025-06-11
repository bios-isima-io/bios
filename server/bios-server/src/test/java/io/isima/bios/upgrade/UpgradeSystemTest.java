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
package io.isima.bios.upgrade;

import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.getCurrentArguments;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.common.BiosConstants;
import io.isima.bios.common.BuildVersion;
import io.isima.bios.common.SharedProperties;
import io.isima.bios.exceptions.ApplicationException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.easymock.IAnswer;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

/** End to end test for upgrade system. */
public class UpgradeSystemTest {
  private static final String NODE_1 = "node1";
  private static final String NODE_2 = "node2";

  private static final String MY_NODE_ID = NODE_1;
  private static final String OTHER_NODE_ID = NODE_2;

  // These are repeated in tests and tests may break if key names change in the upgrade module.
  // This is done to avoid exposing key names outside the upgrade module as public (just for tests)
  // thereby hiding and preventing the use of these keys beyond its intended use.
  private static final String CURRENT_CLUSTER_VERSION_KEY = "currentClusterVersion";
  private static final String NODE_ID_DELIMITER = ".";
  private static final String CURRENT_NODE_VERSION_KEY = "currentNodeVersion";
  private static final String OTHER_NODE_VERSION_KEY =
      OTHER_NODE_ID + NODE_ID_DELIMITER + CURRENT_NODE_VERSION_KEY;

  private AdminInternal adminMock;
  private SharedProperties sharedProperties;
  private UpgradeSystem upgradeSystemUnderTest;

  @Before
  public void setup() {
    adminMock = mock(AdminInternal.class);
    sharedProperties = new SharedProperties.DefaultSharedProperties();
    upgradeSystemUnderTest = new UpgradeSystem(sharedProperties, adminMock, MY_NODE_ID);
  }

  @Test
  public void testNoUpgrade() throws Exception {
    expect(adminMock.getStream(anyString(), anyString())).andThrow(new AssertionError());
    adminMock.removeStream(anyString(), anyString(), anyObject(), anyLong());
    expectLastCall().andThrow(new AssertionError());
    replay(adminMock);

    sharedProperties.setProperty(CURRENT_CLUSTER_VERSION_KEY, BuildVersion.VERSION);
    upgradeSystemUnderTest.upgrade();
  }

  @Test
  public void testClusterUpgrade() throws Exception {
    List<String> expectedTenants = Arrays.asList("t1", "t2", BiosConstants.TENANT_SYSTEM, "t3");
    Set<String> actualTenants = new HashSet<>();
    Set<String> actualStreams = new HashSet<>();
    setupMocks(expectedTenants, actualTenants, actualStreams, -1, 0);
    upgradeSystemUnderTest.upgrade();
    assertThat(actualStreams.size(), Matchers.greaterThanOrEqualTo(1));
    assertThat(actualTenants.size(), is(expectedTenants.size()));
    assertTrue(actualTenants.contains("t1"));
    assertTrue(actualTenants.contains("t2"));
    assertTrue(actualTenants.contains("t3"));
    assertTrue(actualTenants.contains(BiosConstants.TENANT_SYSTEM));
    verify(adminMock);
  }

  @Test
  public void testContinueClusterUpgrade() throws Exception {
    List<String> expectedTenants = Arrays.asList("t1", "t2", BiosConstants.TENANT_SYSTEM, "t3");
    Set<String> actualTenants = new HashSet<>();
    Set<String> actualStreams = new HashSet<>();
    setupMocks(expectedTenants, actualTenants, actualStreams, 2, 1);
    try {
      upgradeSystemUnderTest.upgrade();
      fail("Upgrade should fail halfway");
    } catch (ApplicationException ignored) {
    }
    upgradeSystemUnderTest.upgrade();
    assertThat(actualStreams.size(), Matchers.greaterThanOrEqualTo(1));
    assertThat(actualTenants.size(), is(expectedTenants.size()));
    assertTrue(actualTenants.contains("t1"));
    assertTrue(actualTenants.contains("t2"));
    assertTrue(actualTenants.contains("t3"));
    assertTrue(actualTenants.contains(BiosConstants.TENANT_SYSTEM));
    verify(adminMock);
  }

  @Test
  public void testClusterUpgradeIsIdempotent() throws Exception {
    List<String> expectedTenants = Arrays.asList("t2", "t3");
    Set<String> actualTenants = new HashSet<>();
    Set<String> actualStreams = new HashSet<>();
    setupMocks(expectedTenants, actualTenants, actualStreams, -1, 0);
    upgradeSystemUnderTest.upgrade();
    assertThat(actualStreams.size(), Matchers.greaterThanOrEqualTo(1));
    verify(adminMock);
    reset(adminMock);

    expect(adminMock.getStream(anyString(), anyString())).andThrow(new AssertionError());
    adminMock.removeStream(anyString(), anyString(), anyObject(), anyLong());
    expectLastCall().andThrow(new AssertionError());
    replay(adminMock);
    upgradeSystemUnderTest.upgrade();
  }

  @Test
  public void testNodeUpgrade() throws Exception {
    final var otherSystem = new UpgradeSystem(sharedProperties, adminMock, OTHER_NODE_ID);
    List<String> expectedTenants = Arrays.asList("t1", "t2");
    Set<String> actualTenants = new HashSet<>();
    Set<String> actualStreams = new HashSet<>();
    setupMocks(expectedTenants, actualTenants, actualStreams, -1, 0);
    upgradeSystemUnderTest.upgrade();
    assertThat(actualStreams.size(), Matchers.greaterThanOrEqualTo(1));
    verify(adminMock);
    reset(adminMock);

    assertNull(sharedProperties.getProperty(OTHER_NODE_VERSION_KEY));
    otherSystem.upgrade();
    assertThat(sharedProperties.getProperty(OTHER_NODE_VERSION_KEY), is(BuildVersion.VERSION));
  }

  private void setupMocks(
      List<String> expectedTenants,
      Set<String> actualTenants,
      Set<String> actualStreams,
      int successTimes,
      int errorTimes)
      throws Exception {
    long version = System.currentTimeMillis();
    IAnswer<? extends StreamDesc> streamAnswer =
        () -> {
          String tenantName = (String) getCurrentArguments()[0];
          String streamName = (String) getCurrentArguments()[1];
          StreamDesc testStreamDesc = new StreamDesc(streamName, version);
          testStreamDesc.setParent(new TenantDesc(tenantName, version, false));
          actualTenants.add(tenantName);
          actualStreams.add(streamName);
          return testStreamDesc;
        };

    expect(adminMock.getAllTenants()).andReturn(expectedTenants).anyTimes();
    expect(adminMock.getStream(anyString(), anyString())).andAnswer(streamAnswer).anyTimes();

    adminMock.removeStream(anyString(), anyString(), anyObject(), anyLong());
    if (successTimes > 0 && errorTimes > 0) {
      expectLastCall()
          .times(successTimes)
          .andThrow(new ApplicationException("Test"))
          .times(errorTimes)
          .andVoid()
          .anyTimes();
    } else if (successTimes > 0) {
      expectLastCall().times(successTimes);
    } else {
      expectLastCall().anyTimes();
    }
    replay(adminMock);
  }
}
