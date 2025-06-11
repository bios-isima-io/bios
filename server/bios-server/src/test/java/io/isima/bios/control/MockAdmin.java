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

/** */
package io.isima.bios.control;

import static io.isima.bios.models.v1.InternalAttributeType.STRING;

import io.isima.bios.admin.v1.impl.AdminImpl;
import io.isima.bios.errors.exception.FileReadException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.metrics.MetricsStreamProvider;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.TenantConfig;
import java.util.ArrayList;
import java.util.List;

/**
 * This class used to be a mock TenantManager implementation to serve TenantManager functionality to
 * unit tests before TenantManagerImpl is done. The class is not necessary anymore after
 * TenantMangerImpl is available, but we keep this for backward compatibility.
 */
public class MockAdmin extends AdminImpl {

  /** ctor. */
  public MockAdmin() throws FileReadException {
    super(null, new MetricsStreamProvider());

    List<AttributeDesc> eventAttributes = new ArrayList<AttributeDesc>();
    AttributeDesc eventAttribute1 = new AttributeDesc("first", STRING);
    eventAttribute1.setDefaultValue("first");
    AttributeDesc eventAttribute2 = new AttributeDesc("second", STRING);
    eventAttribute2.setDefaultValue("second");

    eventAttributes.add(eventAttribute1);
    eventAttributes.add(eventAttribute2);

    List<AttributeDesc> additionalEventAttributes = new ArrayList<AttributeDesc>();
    AttributeDesc additionalAttributeCity = new AttributeDesc("city", STRING);
    additionalAttributeCity.setDefaultValue("Fremont");
    additionalEventAttributes.add(additionalAttributeCity);

    AttributeDesc additionalAttributeState = new AttributeDesc("state", STRING);
    additionalAttributeState.setDefaultValue("California");
    additionalEventAttributes.add(additionalAttributeState);

    AttributeDesc additionalAttributeCountry = new AttributeDesc("country", STRING);
    additionalAttributeCountry.setDefaultValue("US");
    additionalEventAttributes.add(additionalAttributeCountry);

    StreamConfig streamConfig = new StreamConfig("hello");
    streamConfig.setAttributes(eventAttributes);
    streamConfig.setAdditionalAttributes(additionalEventAttributes);

    TenantConfig tenantConfig = new TenantConfig();
    tenantConfig.setName("elasticflash");

    List<StreamConfig> streams = new ArrayList<StreamConfig>();
    streams.add(streamConfig);
    tenantConfig.setStreams(streams);

    try {
      addTenant(tenantConfig, RequestPhase.FINAL, System.currentTimeMillis());
    } catch (TfosException | ApplicationException e) {
      // shouldn't happen
    }
  }
}
