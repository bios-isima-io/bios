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
package io.isima.bios.models.v1;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import io.isima.bios.models.Duplicatable;
import io.isima.bios.models.v1.validators.ValidTenantConfig;
import io.isima.bios.models.v1.validators.ValidatorConstants;
import java.util.ArrayList;
import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

/*
 * Tenant configuration includes name and stream config.
 */
@Getter
@Setter
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
@ValidTenantConfig
public class TenantConfig extends Duplicatable {

  /*
   * Tenant name.
   */
  @NotNull
  @Size(min = 1, max = 40)
  @Pattern(regexp = ValidatorConstants.NAME_PATTERN)
  private String name;

  @Accessors(chain = true)
  @JsonIgnore
  private String domain;

  /*
   * Tenant can have multiple streams defined.
   */
  @Valid private List<StreamConfig> streams;

  @Accessors(chain = true)
  private Long version;

  @Accessors(chain = true)
  @JsonIgnore
  private Boolean deleted;

  @Accessors(chain = true)
  @JsonIgnore
  private String appMaster;

  /**
   * Method to duplicate the object.
   *
   * <p>Modification in the duplicated object does not affect the original object.
   *
   * @return Duplicated instance.
   */
  @Override
  public TenantConfig duplicate() {
    TenantConfig dup = new TenantConfig(name);
    dup.domain = domain;
    dup.version = version;
    dup.deleted = deleted;
    streams.forEach(stream -> dup.streams.add(stream.duplicate()));
    dup.appMaster = appMaster;
    return dup;
  }

  /** Default constructor. */
  public TenantConfig() {
    streams = new ArrayList<>();
  }

  /**
   * Constructor with name.
   *
   * @param name Tenant name
   */
  public TenantConfig(String name) {
    this.name = name;
    streams = new ArrayList<>();
  }

  /**
   * Add stream to the tenant.
   *
   * @param stream Stream to add
   * @return Self
   */
  public TenantConfig addStream(StreamConfig stream) {
    if (streams == null) {
      streams = new ArrayList<>();
    }
    // Replace if there is an existing one
    for (int i = 0; i < streams.size(); ++i) {
      if (streams.get(i).getName().equalsIgnoreCase(stream.getName())) {
        streams.set(i, stream);
        return this;
      }
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
  public static boolean validate(TenantConfig tenantConfig) {
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
