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
package io.isima.bios.admin.v1;

import io.isima.bios.data.impl.storage.CassStream;
import io.isima.bios.models.Rollup;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.storage.cassandra.CassandraConnection;
import java.util.List;
import org.junit.Assert;

public class MaintenanceTestUtils {

  public static void verifySubstreamTables(
      CassandraConnection conn,
      StreamConfig streamConfig,
      String keyspace,
      boolean isViewExpected,
      boolean isRollupExpected) {
    streamConfig
        .getViews()
        .forEach(
            viewDesc -> {
              String viewQualifiedName =
                  CassStream.generateQualifiedNameSubstream(
                      StreamType.VIEW, streamConfig.getName(), viewDesc.getName());
              String viewTableName =
                  CassStream.generateTableName(
                      StreamType.VIEW, viewQualifiedName, streamConfig.getVersion());
              String indexQualifiedName =
                  CassStream.generateQualifiedNameSubstream(
                      StreamType.INDEX, streamConfig.getName(), viewDesc.getName());
              String indexTableName =
                  CassStream.generateTableName(
                      StreamType.INDEX, indexQualifiedName, streamConfig.getVersion());
              if (isViewExpected) {
                Assert.assertTrue(conn.verifyTable(keyspace, viewTableName));
                Assert.assertTrue(conn.verifyTable(keyspace, indexTableName));
              } else {
                Assert.assertFalse(conn.verifyTable(keyspace, viewTableName));
                Assert.assertFalse(conn.verifyTable(keyspace, indexTableName));
              }
            });

    streamConfig
        .getPostprocesses()
        .forEach(
            postprocessDesc -> {
              List<Rollup> rollupList = postprocessDesc.getRollups();
              if (rollupList != null) {
                rollupList.forEach(
                    rollup -> {
                      String rollUpQualifiedName =
                          CassStream.generateQualifiedNameSubstream(
                              StreamType.ROLLUP, streamConfig.getName(), rollup.getName());
                      String rollUpTableName =
                          CassStream.generateTableName(
                              StreamType.ROLLUP, rollUpQualifiedName, streamConfig.getVersion());
                      if (isRollupExpected) {
                        Assert.assertTrue(conn.verifyTable(keyspace, rollUpTableName));
                      } else {
                        Assert.assertFalse(conn.verifyTable(keyspace, rollUpTableName));
                      }
                    });
              }
            });
  }
}
