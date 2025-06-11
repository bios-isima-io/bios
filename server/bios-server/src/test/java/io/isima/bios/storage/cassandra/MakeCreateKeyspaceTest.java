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
package io.isima.bios.storage.cassandra;

import static org.junit.Assert.assertEquals;

import io.isima.bios.exceptions.ApplicationException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class MakeCreateKeyspaceTest {

  private static String previousStrategy;
  private static String previousDataCenter;
  private static String previousDataCenters;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    previousStrategy = System.getProperty(CassandraConfig.CASSANDRA_USE_NETWORK_TOPOLOGY_STRATEGY);
    previousDataCenter = System.getProperty(CassandraConfig.CASSANDRA_DATA_CENTER);
    previousDataCenters = System.getProperty(CassandraConfig.CASSANDRA_DATA_CENTERS);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    setPropertyOrClear(CassandraConfig.CASSANDRA_USE_NETWORK_TOPOLOGY_STRATEGY, previousStrategy);
    setPropertyOrClear(CassandraConfig.CASSANDRA_DATA_CENTER, previousDataCenter);
    setPropertyOrClear(CassandraConfig.CASSANDRA_DATA_CENTERS, previousDataCenters);
  }

  private static void setPropertyOrClear(String key, String value) {
    if (value != null) {
      System.setProperty(key, value);
    } else {
      System.clearProperty(key);
    }
  }

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testDatacenters() throws ApplicationException {
    System.setProperty(CassandraConfig.CASSANDRA_USE_NETWORK_TOPOLOGY_STRATEGY, "true");
    System.setProperty(
        CassandraConfig.CASSANDRA_DATA_CENTERS, "us-west1_signal:1,us-west1_summary:2");
    final String keyspace = "keyspace123";
    final String query = CassandraConnection.makeCreateKeyspaceQuery(keyspace, 1);
    assertEquals(
        "CREATE KEYSPACE IF NOT EXISTS keyspace123"
            + " WITH replication = {'class': 'NetworkTopologyStrategy',"
            + " 'us-west1_signal': '1', 'us-west1_summary': '2'} AND durable_writes = true",
        query);
  }

  @Test
  public void testDatacenter() throws ApplicationException {
    System.setProperty(CassandraConfig.CASSANDRA_USE_NETWORK_TOPOLOGY_STRATEGY, "true");
    System.setProperty(CassandraConfig.CASSANDRA_DATA_CENTER, "us-central1_signal");
    final String keyspace = "keyspace123";
    final String query = CassandraConnection.makeCreateKeyspaceQuery(keyspace, 3);
    assertEquals(
        "CREATE KEYSPACE IF NOT EXISTS keyspace123"
            + " WITH replication = {'class': 'NetworkTopologyStrategy',"
            + " 'us-central1_signal': '3'} AND durable_writes = true",
        query);
  }
}
