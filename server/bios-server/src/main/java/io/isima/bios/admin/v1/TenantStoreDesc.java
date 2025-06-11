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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import io.isima.bios.models.Duplicatable;
import io.isima.bios.models.v1.StreamConfig;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

/*
 * Tenant store descriptor
 */
@Getter
@Setter
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class TenantStoreDesc extends Duplicatable {

  /** Tenant name. */
  private String name;

  /*
   * Tenant can have multiple streams defined.
   */
  private List<StreamStoreDesc> streams;

  @Accessors(chain = true)
  private Long version;

  /**
   * StreamStoreDesc stores streamNameProxy for each stream in a tenant. maxAllocatedStreamNameProxy
   * is the maximum such allocated stream name proxy number for the whole tenant, across versions.
   * Keeping track of this ensures we do not allocate the same proxy number to two different streams
   * even if one is deleted before creating another.
   */
  private Integer maxAllocatedStreamNameProxy;

  @Accessors(chain = true)
  @JsonIgnore
  private Boolean deleted;

  @Accessors(chain = true)
  private String appMaster;

  /**
   * Method to duplicate the object.
   *
   * <p>Modification in the duplicated object does not affect the original object.
   *
   * @return Duplicated instance.
   */
  @Override
  public TenantStoreDesc duplicate() {
    TenantStoreDesc dup = new TenantStoreDesc(name);
    streams.forEach(stream -> dup.streams.add(stream.duplicate()));
    dup.version = version;
    dup.maxAllocatedStreamNameProxy = maxAllocatedStreamNameProxy;
    dup.deleted = deleted;
    dup.appMaster = appMaster;
    return dup;
  }

  /** Default constructor. */
  public TenantStoreDesc() {
    streams = new ArrayList<>();
  }

  /**
   * Constructor with name.
   *
   * @param name Tenant name
   */
  public TenantStoreDesc(String name) {
    this.name = name;
    streams = new ArrayList<>();
  }

  /**
   * Add stream to the tenant.
   *
   * @param stream Stream to add
   * @return Self
   */
  public TenantStoreDesc addStream(StreamStoreDesc stream) {
    if (streams == null) {
      streams = new ArrayList<>();
    }
    streams.add(stream);
    return this;
  }

  public boolean isDeleted() {
    return deleted != null ? deleted : false;
  }

  /**
   * Checks tenantConfig validity.
   *
   * <p>The method checks - if the object exists - if the tenant config has a name - if the tenant
   * config has the list of streams (empty list is fine) - if each stream is valid TODO: This will
   * be removed and is only as a debugging aid.
   *
   * @param tenantConfig Tenant configuration to validate
   * @return Whether if the given configuration is valid.
   */
  public static boolean validate(TenantStoreDesc tenantConfig) {
    if (tenantConfig == null
        || tenantConfig.getName() == null
        || tenantConfig.getName().isEmpty()) {
      return false;
    }
    if (tenantConfig.getStreams() == null) {
      return false;
    }
    for (StreamConfig streamConfig : tenantConfig.getStreams()) {
      if (!StreamConfig.validate(streamConfig)) {
        return false;
      }
    }
    return true;
  }
}
