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
package io.isima.bios.deli.importer;

import io.isima.bios.deli.models.SslConfiguration;
import java.util.Collection;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CdcConfParams {
  private String databaseName;

  private SslConfiguration sslConfig;

  private Collection<String> tables;

  /**
   * Generate a list of table names separated by comma.
   *
   * <p>If tables are not specified or databaseName is null, the method returns null</p>
   */
  public String getTableList() {
    if (databaseName == null || tables == null) {
      return null;
    }
    return String.join(",", tables.stream()
        .map((tableName) -> databaseName + "." + tableName)
        .collect(Collectors.toList()));
  }
}
