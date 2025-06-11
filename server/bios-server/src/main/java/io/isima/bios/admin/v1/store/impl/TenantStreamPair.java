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
package io.isima.bios.admin.v1.store.impl;

import java.util.Objects;
import lombok.Getter;

@Getter
public class TenantStreamPair {
  private String tenant;
  private Long tenantVersion;
  private String stream;

  public TenantStreamPair(String tenant, Long tenantVersion, String stream) {
    this.tenant = tenant;
    this.tenantVersion = tenantVersion;
    this.stream = stream;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof TenantStreamPair)) {
      return false;
    }

    TenantStreamPair other = (TenantStreamPair) o;
    return tenant.equals(other.tenant)
        && tenantVersion.equals(other.tenantVersion)
        && stream.equals(other.stream);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tenant, tenantVersion, stream);
  }
}
