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
package io.isima.bios.data.storage.cassandra;

import static org.junit.Assert.assertEquals;

import io.isima.bios.data.impl.storage.CassStream;
import io.isima.bios.models.v1.StreamType;
import java.util.Collection;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

public class CassandraStoreUtilsTest {

  @RunWith(Parameterized.class)
  public static class KeyspaceNameCreationTest {
    @Parameterized.Parameter(0)
    public String tenantName;

    @Parameterized.Parameter(1)
    public Long version;

    @Parameterized.Parameter(2)
    public String expected;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
      return List.of(
          new Object[][] {
            {"isima", 1602776720315L, "tfos_d_64fbbae171403b75bd815191cd5e24dc"},
            {"ISIMA", 1602776720315L, "tfos_d_64fbbae171403b75bd815191cd5e24dc"},
            {"isima", 1602777859879L, "tfos_d_eaa3fe2c1bde3d73b6181922460b9ee4"},
            {"_system", 1602777859879L, "tfos_d_5931780c3db23b0e84ac185474fb266d"},
          });
    }

    @Test
    public void test() {
      assertEquals(expected, CassandraDataStoreUtils.generateKeyspaceName(tenantName, version));
    }
  }

  @RunWith(Parameterized.class)
  public static class TableNameCreationTest {
    @Parameterized.Parameter(0)
    public StreamType type;

    @Parameterized.Parameter(1)
    public String entityName;

    @Parameterized.Parameter(2)
    public Long version;

    @Parameterized.Parameter(3)
    public String expected;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
      return List.of(
          new Object[][] {
            {
              StreamType.SIGNAL,
              "productOrder",
              1602776720315L,
              "evt_6feffab2bbda3efbb02453df4b83acd5"
            },
            {StreamType.CONTEXT, "geoIp", 1602776720315L, "cx2_b3ce3b40e8a437878f62ca2acd234cb1"},
            {StreamType.INDEX, "my.index", 1602777859879L, "idx_d19b52ee0135339ea7c7075ae0e54c25"},
            {StreamType.VIEW, "my.view", 1602777859879L, "ivw_74f43a11df0436128c42f73838d621f1"},
            {
              StreamType.ROLLUP,
              "productOrder.my.rollup",
              1602777859879L,
              "rlp_72e1517621ad31ec8fb927920187f7fc"
            },
          });
    }

    @Test
    public void test() {
      assertEquals(expected, CassStream.generateTableName(type, entityName, version));
      assertEquals(expected, CassStream.generateTableName(type, entityName.toLowerCase(), version));
      assertEquals(expected, CassStream.generateTableName(type, entityName.toUpperCase(), version));
    }
  }
}
