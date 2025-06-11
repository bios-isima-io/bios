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
package io.isima.bios.admin;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.exceptions.validator.ConstraintViolationValidatorException;
import io.isima.bios.exceptions.validator.NotImplementedValidatorException;
import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.EnrichConfig;
import io.isima.bios.models.EnrichmentAttribute;
import io.isima.bios.models.EnrichmentConfigSignal;
import io.isima.bios.models.MissingAttributePolicy;
import io.isima.bios.models.MissingAttributePolicyV1;
import io.isima.bios.models.MissingLookupPolicy;
import io.isima.bios.models.SignalConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class StreamConfigTranslatorTest {

  @Test
  public void toTfos()
      throws NotImplementedValidatorException,
          ConstraintViolationValidatorException,
          JsonProcessingException {
    var objectMapper = new ObjectMapper();
    var signalConfig = new SignalConfig();
    signalConfig.setMissingAttributePolicy(MissingAttributePolicy.STORE_DEFAULT_VALUE);
    List<AttributeConfig> attributes = new ArrayList<>();
    var attribute = new AttributeConfig("test", AttributeType.INTEGER);
    attributes.add(attribute);
    signalConfig.setAttributes(attributes);

    //    signalConfig.setAttributes();
    signalConfig.setName("TestName");
    var enrich = new EnrichConfig();
    enrich.setMissingLookupPolicy(MissingLookupPolicy.REJECT);
    signalConfig.setEnrich(enrich);
    var enrichment = new EnrichmentConfigSignal();
    enrichment.setName("Test");
    enrichment.setForeignKey(Arrays.asList("Test"));
    enrichment.setContextName("Test");
    var ea = new EnrichmentAttribute();
    ea.setAttributeName("TestName");

    enrichment.setContextAttributes(Arrays.asList(ea));

    enrich.setEnrichments(Arrays.asList(enrichment));
    var streamConfig = StreamConfigTranslator.toTfos(signalConfig);
    assertEquals(
        MissingAttributePolicyV1.STRICT,
        streamConfig.getPreprocesses().get(0).getMissingLookupPolicy());
    enrich.setMissingLookupPolicy(MissingLookupPolicy.STORE_FILL_IN_VALUE);
    streamConfig = StreamConfigTranslator.toTfos(signalConfig);
    assertEquals(
        MissingAttributePolicyV1.USE_DEFAULT,
        streamConfig.getPreprocesses().get(0).getMissingLookupPolicy());
    System.err.println(objectMapper.writeValueAsString(streamConfig));
  }
}
