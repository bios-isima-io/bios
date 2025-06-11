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
package io.isima.bios.models.auth;

import io.isima.bios.models.BaseModel;
import java.util.Objects;

public class Organization extends BaseModel {
  public static final String TABLE_NAME = "bi_organizations";
  public static final String KEY_NAME = "Organization";

  private Long id;
  private String tenant = "";
  private String settings;

  public Organization() {
    this.key = KEY_NAME;
    this.setName("default");
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getTenant() {
    return tenant;
  }

  public void setTenant(String tenant) {
    this.tenant = tenant;
  }

  public String getSettings() {
    return settings;
  }

  public void setSettings(String settings) {
    this.settings = settings;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    Organization other = (Organization) obj;
    return Objects.equals(this.getKey(), other.getKey())
        && Objects.equals(this.id, other.id)
        && Objects.equals(this.getName(), other.getName())
        && Objects.equals(this.tenant, other.tenant)
        && Objects.equals(this.settings, other.settings);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        this.getKey(), this.getId(), this.getName(), this.getTenant(), this.getSettings());
  }
}
