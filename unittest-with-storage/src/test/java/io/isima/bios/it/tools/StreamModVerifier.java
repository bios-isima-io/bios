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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.AdminStore;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.impl.AdminImpl;
import io.isima.bios.common.FormatVersion;
import io.isima.bios.data.impl.DataEngineImpl;
import io.isima.bios.data.impl.storage.CassStream;
import io.isima.bios.metrics.MetricsStreamProvider;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.storage.cassandra.CassandraConnection;

public abstract class StreamModVerifier {

  protected final AdminInternal admin;
  protected final DataEngineImpl dataEngine;
  protected final AdminStore adminStore;
  protected final CassandraConnection conn;

  protected final String testTenant;

  public StreamModVerifier(
      AdminInternal admin,
      DataEngineImpl dataEngine,
      AdminStore adminStore,
      CassandraConnection conn,
      String testTenant) {
    this.admin = admin;
    this.dataEngine = dataEngine;
    this.adminStore = adminStore;
    this.testTenant = testTenant;
    this.conn = conn;
  }

  public void run(StreamConfig newConf, StreamConfig origConf) throws Exception {
    verify(admin, dataEngine, newConf, origConf);

    DataEngineImpl reloadedEngine = new DataEngineImpl(conn);
    final AdminImpl reloadedAdmin =
        new AdminImpl(adminStore, new MetricsStreamProvider(), reloadedEngine);
    verify(reloadedAdmin, reloadedEngine, newConf, origConf);

    admin.reload();
    verify(admin, dataEngine, newConf, origConf);
  }

  protected void verify(
      AdminInternal admin, DataEngineImpl dataEngine, StreamConfig newConf, StreamConfig origConf)
      throws Exception {
    // Check AdminInternal
    final StreamDesc newDesc = admin.getStream(testTenant, newConf.getName());
    basicCheck(newDesc, newConf);
    checkNew(newDesc, newConf, origConf);
    if (hasPrev()) {
      basicCheck(newDesc.getPrev(), origConf);
      assertEquals(origConf.getName(), newDesc.getPrevName());
      assertEquals(origConf.getVersion(), newDesc.getPrevVersion());
      checkOld(newDesc.getPrev(), newConf, origConf);
    } else {
      assertNull(newDesc.getPrev());
    }

    // Check Data Engine
    final CassStream newCassStream = dataEngine.getCassStream(newDesc);
    assertNotNull(newCassStream);
    checkNew(newCassStream.getStreamDesc(), newConf, origConf);
    if (hasPrev()) {
      final CassStream oldCassStream = dataEngine.getCassStream(newDesc.getPrev());
      assertNotNull(oldCassStream);
      assertSame(oldCassStream, newCassStream.getPrev());
      basicCheck(oldCassStream.getStreamDesc(), origConf);
      checkOld(oldCassStream.getStreamDesc(), newConf, origConf);
    } else {
      assertNull(newCassStream.getPrev());
      assertNull(newCassStream.getStreamDesc().getPrev());
    }

    extraCheck(admin, dataEngine, newConf, origConf);
  }

  private void basicCheck(StreamDesc desc, StreamConfig conf) {
    assertNotNull(desc);
    assertEquals(conf.getName(), desc.getName());
    assertEquals(conf.getVersion(), desc.getVersion());
    assertEquals(FormatVersion.LATEST, desc.getFormatVersion());
  }

  protected abstract void checkNew(StreamDesc newDesc, StreamConfig newConf, StreamConfig origConf);

  protected abstract boolean hasPrev();

  protected abstract void checkOld(StreamDesc newDesc, StreamConfig newConf, StreamConfig origConf);

  protected void extraCheck(
      AdminInternal admin, DataEngineImpl dataEngine, StreamConfig newConf, StreamConfig origConfig)
      throws Exception {}
}
