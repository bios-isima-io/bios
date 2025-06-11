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
package io.isima.bios.integrations.validator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.exceptions.validator.ConstraintViolationValidatorException;
import io.isima.bios.exceptions.validator.MultipleValidatorViolationsException;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.DataPickupSpec;
import io.isima.bios.models.ImportDestinationConfig;
import io.isima.bios.models.ImportFlowConfig;
import io.isima.bios.models.ImportSourceConfig;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.TenantConfig;
import java.util.List;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ImportFlowSpecValidatorTest {

  private static ObjectMapper mapper;

  private TenantConfig tenantConfig;
  private ImportFlowConfig flowSpec;
  private ImportFlowConfig flowSpec2;
  private ImportFlowConfig flowSpec3;

  @BeforeClass
  public static void setUpClass() {
    mapper = new ObjectMapper();
  }

  @Before
  public void setUp() throws Exception {
    final String contextSrc =
        "{"
            + "  'contextName': 'campaignContext',"
            + "  'missingAttributePolicy': 'Reject',"
            + "  'attributes': ["
            + "    {"
            + "      'attributeName': 'campaignId',"
            + "      'type': 'String'"
            + "    },"
            + "    {"
            + "      'attributeName': 'name',"
            + "      'type': 'String',"
            + "      'category': 'Description',"
            + "      'default': 'NA'"
            + "    },"
            + "    {"
            + "      'attributeName': 'source',"
            + "      'type': 'String',"
            + "      'category': 'Description',"
            + "      'missingAttributePolicy': 'StoreDefaultValue',"
            + "      'default': 'NA'"
            + "    },"
            + "    {"
            + "      'attributeName': 'medium',"
            + "      'type': 'String',"
            + "      'category': 'Description',"
            + "      'default': 'NA'"
            + "    },"
            + "    {"
            + "      'attributeName': 'term',"
            + "      'type': 'String',"
            + "      'category': 'Description',"
            + "      'default': 'NA'"
            + "    },"
            + "    {"
            + "      'attributeName': 'content',"
            + "      'type': 'String',"
            + "      'category': 'Description',"
            + "      'default': 'NA'"
            + "    }"
            + "  ],"
            + "  'primaryKey': ["
            + "    'campaignId'"
            + "  ]"
            + "}";
    final var context = mapper.readValue(contextSrc.replace("'", "\""), ContextConfig.class);

    final String signalSrc =
        "{\n"
            + "  \"signalName\": \"eventSignal\",\n"
            + "  \"missingAttributePolicy\": \"Reject\",\n"
            + "  \"attributes\": [\n"
            + "    {\n"
            + "      \"attributeName\": \"type\",\n"
            + "      \"type\": \"String\",\n"
            + "      \"category\": \"Id\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"attributeName\": \"event\",\n"
            + "      \"type\": \"String\",\n"
            + "      \"category\": \"Id\",\n"
            + "      \"missingAttributePolicy\": \"StoreDefaultValue\",\n"
            + "      \"default\": \"NA\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"attributeName\": \"channel\",\n"
            + "      \"type\": \"String\",\n"
            + "      \"category\": \"Id\",\n"
            + "      \"missingAttributePolicy\": \"StoreDefaultValue\",\n"
            + "      \"default\": \"UNKNOWN\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"attributeName\": \"name\",\n"
            + "      \"type\": \"String\",\n"
            + "      \"category\": \"Id\",\n"
            + "      \"missingAttributePolicy\": \"StoreDefaultValue\",\n"
            + "      \"default\": \"MISSING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"attributeName\": \"category\",\n"
            + "      \"type\": \"String\",\n"
            + "      \"category\": \"Id\",\n"
            + "      \"missingAttributePolicy\": \"StoreDefaultValue\",\n"
            + "      \"default\": \"MISSING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"attributeName\": \"anonymousId\",\n"
            + "      \"type\": \"String\",\n"
            + "      \"category\": \"Id\",\n"
            + "      \"missingAttributePolicy\": \"StoreDefaultValue\",\n"
            + "      \"default\": \"MISSING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"attributeName\": \"messageId\",\n"
            + "      \"type\": \"String\",\n"
            + "      \"category\": \"Id\",\n"
            + "      \"missingAttributePolicy\": \"StoreDefaultValue\",\n"
            + "      \"default\": \"MISSING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"attributeName\": \"projectId\",\n"
            + "      \"type\": \"String\",\n"
            + "      \"category\": \"Id\",\n"
            + "      \"missingAttributePolicy\": \"StoreDefaultValue\",\n"
            + "      \"default\": \"MISSING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"attributeName\": \"version\",\n"
            + "      \"type\": \"String\",\n"
            + "      \"category\": \"Description\",\n"
            + "      \"missingAttributePolicy\": \"StoreDefaultValue\",\n"
            + "      \"default\": \"NA\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"attributeName\": \"originalTimestamp\",\n"
            + "      \"type\": \"String\",\n"
            + "      \"category\": \"Description\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"attributeName\": \"timestamp\",\n"
            + "      \"type\": \"String\",\n"
            + "      \"category\": \"Description\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"attributeName\": \"receivedAt\",\n"
            + "      \"type\": \"String\",\n"
            + "      \"category\": \"Description\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"attributeName\": \"sentAt\",\n"
            + "      \"type\": \"String\",\n"
            + "      \"category\": \"Description\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"attributeName\": \"pagePath\",\n"
            + "      \"type\": \"String\",\n"
            + "      \"category\": \"Description\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"attributeName\": \"pageReferrer\",\n"
            + "      \"type\": \"String\",\n"
            + "      \"category\": \"Description\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"attributeName\": \"pageSearch\",\n"
            + "      \"type\": \"String\",\n"
            + "      \"category\": \"Description\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"attributeName\": \"pageTitle\",\n"
            + "      \"type\": \"String\",\n"
            + "      \"category\": \"Description\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"attributeName\": \"pageUrl\",\n"
            + "      \"type\": \"String\",\n"
            + "      \"category\": \"Description\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"attributeName\": \"userId\",\n"
            + "      \"type\": \"String\",\n"
            + "      \"category\": \"Id\",\n"
            + "      \"missingAttributePolicy\": \"StoreDefaultValue\",\n"
            + "      \"default\": \"MISSING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"attributeName\": \"productId\",\n"
            + "      \"type\": \"String\",\n"
            + "      \"category\": \"Id\",\n"
            + "      \"missingAttributePolicy\": \"StoreDefaultValue\",\n"
            + "      \"default\": \"NA\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"attributeName\": \"productCurrency\",\n"
            + "      \"type\": \"String\",\n"
            + "      \"category\": \"Id\",\n"
            + "      \"missingAttributePolicy\": \"StoreDefaultValue\",\n"
            + "      \"default\": \"NA\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"attributeName\": \"productPrice\",\n"
            + "      \"type\": \"Decimal\",\n"
            + "      \"category\": \"Gauge\",\n"
            + "      \"missingAttributePolicy\": \"StoreDefaultValue\",\n"
            + "      \"default\": 0.0,\n"
            + "      \"positiveIndicator\": \"High\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"attributeName\": \"productQty\",\n"
            + "      \"type\": \"Integer\",\n"
            + "      \"category\": \"Gauge\",\n"
            + "      \"missingAttributePolicy\": \"StoreDefaultValue\",\n"
            + "      \"default\": 0,\n"
            + "      \"positiveIndicator\": \"High\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"attributeName\": \"productValue\",\n"
            + "      \"type\": \"Decimal\",\n"
            + "      \"category\": \"Gauge\",\n"
            + "      \"missingAttributePolicy\": \"StoreDefaultValue\",\n"
            + "      \"default\": 0.0,\n"
            + "      \"positiveIndicator\": \"High\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"attributeName\": \"campaignId\",\n"
            + "      \"type\": \"String\",\n"
            + "      \"category\": \"Id\",\n"
            + "      \"missingAttributePolicy\": \"StoreDefaultValue\",\n"
            + "      \"default\": \"UNKNOWN\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"enrich\": {\n"
            + "    \"enrichments\": [\n"
            + "      {\n"
            + "        \"enrichmentName\": \"userEnrichment\",\n"
            + "        \"foreignKey\": [\n"
            + "          \"userId\"\n"
            + "        ],\n"
            + "        \"missingLookupPolicy\": \"StoreFillInValue\",\n"
            + "        \"contextName\": \"userContext\",\n"
            + "        \"contextAttributes\": [\n"
            + "          {\n"
            + "            \"attributeName\": \"email\",\n"
            + "            \"as\": \"userEmail\",\n"
            + "            \"fillIn\": \"UNKNOWN\"\n"
            + "          },\n"
            + "          {\n"
            + "            \"attributeName\": \"ip\",\n"
            + "            \"as\": \"userIP\",\n"
            + "            \"fillIn\": \"UNKNOWN\"\n"
            + "          },\n"
            + "          {\n"
            + "            \"attributeName\": \"locale\",\n"
            + "            \"as\": \"userLocale\",\n"
            + "            \"fillIn\": \"UNKNOWN\"\n"
            + "          },\n"
            + "          {\n"
            + "            \"attributeName\": \"userAgent\",\n"
            + "            \"as\": \"userAgent\",\n"
            + "            \"fillIn\": \"MISSING\"\n"
            + "          }\n"
            + "        ]\n"
            + "      },\n"
            + "      {\n"
            + "        \"enrichmentName\": \"productEnrichment\",\n"
            + "        \"foreignKey\": [\n"
            + "          \"productId\"\n"
            + "        ],\n"
            + "        \"missingLookupPolicy\": \"StoreFillInValue\",\n"
            + "        \"contextName\": \"productContext\",\n"
            + "        \"contextAttributes\": [\n"
            + "          {\n"
            + "            \"attributeName\": \"productName\",\n"
            + "            \"as\": \"productName\",\n"
            + "            \"fillIn\": \"N/A\"\n"
            + "          },\n"
            + "          {\n"
            + "            \"attributeName\": \"productBrand\",\n"
            + "            \"as\": \"productBrand\",\n"
            + "            \"fillIn\": \"N/A\"\n"
            + "          },\n"
            + "          {\n"
            + "            \"attributeName\": \"productCategory\",\n"
            + "            \"as\": \"productCategory\",\n"
            + "            \"fillIn\": \"N/A\"\n"
            + "          },\n"
            + "          {\n"
            + "            \"attributeName\": \"productImageURL\",\n"
            + "            \"as\": \"productImageURL\",\n"
            + "            \"fillIn\": \"N/A\"\n"
            + "          },\n"
            + "          {\n"
            + "            \"attributeName\": \"productURL\",\n"
            + "            \"as\": \"productURL\",\n"
            + "            \"fillIn\": \"N/A\"\n"
            + "          }\n"
            + "        ]\n"
            + "      },\n"
            + "      {\n"
            + "        \"enrichmentName\": \"campaignEnrichment\",\n"
            + "        \"foreignKey\": [\n"
            + "          \"campaignId\"\n"
            + "        ],\n"
            + "        \"missingLookupPolicy\": \"StoreFillInValue\",\n"
            + "        \"contextName\": \"campaignContext\",\n"
            + "        \"contextAttributes\": [\n"
            + "          {\n"
            + "            \"attributeName\": \"medium\",\n"
            + "            \"as\": \"campaignMedium\",\n"
            + "            \"fillIn\": \"UNKNOWN\"\n"
            + "          },\n"
            + "          {\n"
            + "            \"attributeName\": \"name\",\n"
            + "            \"as\": \"campaignName\",\n"
            + "            \"fillIn\": \"UNKNOWN\"\n"
            + "          },\n"
            + "          {\n"
            + "            \"attributeName\": \"source\",\n"
            + "            \"as\": \"campaignSource\",\n"
            + "            \"fillIn\": \"UNKNOWN\"\n"
            + "          },\n"
            + "          {\n"
            + "            \"attributeName\": \"term\",\n"
            + "            \"as\": \"campaingnTerm\",\n"
            + "            \"fillIn\": \"UNKNOWN\"\n"
            + "          },\n"
            + "          {\n"
            + "            \"attributeName\": \"content\",\n"
            + "            \"as\": \"campaignContent\",\n"
            + "            \"fillIn\": \"UNKNOWN\"\n"
            + "          }\n"
            + "        ]\n"
            + "      }\n"
            + "    ]\n"
            + "  },\n"
            + "  \"postStorageStage\": {\n"
            + "    \"features\": [\n"
            + "      {\n"
            + "        \"featureName\": \"defaultCount\",\n"
            + "        \"attributes\": [\n"
            + "          \"productPrice\",\n"
            + "          \"productQty\",\n"
            + "          \"productValue\"\n"
            + "        ],\n"
            + "        \"featureInterval\": 300000\n"
            + "      },\n"
            + "      {\n"
            + "        \"featureName\": \"byTypeEventChannel\",\n"
            + "        \"dimensions\": [\n"
            + "          \"type\",\n"
            + "          \"event\",\n"
            + "          \"channel\"\n"
            + "        ],\n"
            + "        \"attributes\": [\n"
            + "          \"productPrice\",\n"
            + "          \"productQty\",\n"
            + "          \"productValue\"\n"
            + "        ],\n"
            + "        \"featureInterval\": 300000\n"
            + "      },\n"
            + "      {\n"
            + "        \"featureName\": \"byPageSearch\",\n"
            + "        \"dimensions\": [\n"
            + "          \"pageSearch\"\n"
            + "        ],\n"
            + "        \"attributes\": [\n"
            + "          \"productPrice\",\n"
            + "          \"productQty\",\n"
            + "          \"productValue\"\n"
            + "        ],\n"
            + "        \"featureInterval\": 300000\n"
            + "      },\n"
            + "      {\n"
            + "        \"featureName\": \"byPageTitle\",\n"
            + "        \"dimensions\": [\n"
            + "          \"pageTitle\"\n"
            + "        ],\n"
            + "        \"attributes\": [\n"
            + "          \"productPrice\",\n"
            + "          \"productQty\",\n"
            + "          \"productValue\"\n"
            + "        ],\n"
            + "        \"featureInterval\": 300000\n"
            + "      },\n"
            + "      {\n"
            + "        \"featureName\": \"byPagePath\",\n"
            + "        \"dimensions\": [\n"
            + "          \"pagePath\"\n"
            + "        ],\n"
            + "        \"attributes\": [\n"
            + "          \"productPrice\",\n"
            + "          \"productQty\",\n"
            + "          \"productValue\"\n"
            + "        ],\n"
            + "        \"featureInterval\": 300000\n"
            + "      },\n"
            + "      {\n"
            + "        \"featureName\": \"byUserEmail\",\n"
            + "        \"dimensions\": [\n"
            + "          \"userEmail\"\n"
            + "        ],\n"
            + "        \"attributes\": [\n"
            + "          \"productPrice\",\n"
            + "          \"productQty\",\n"
            + "          \"productValue\"\n"
            + "        ],\n"
            + "        \"featureInterval\": 300000\n"
            + "      },\n"
            + "      {\n"
            + "        \"featureName\": \"byUserIP\",\n"
            + "        \"dimensions\": [\n"
            + "          \"userIP\"\n"
            + "        ],\n"
            + "        \"attributes\": [\n"
            + "          \"productPrice\",\n"
            + "          \"productQty\",\n"
            + "          \"productValue\"\n"
            + "        ],\n"
            + "        \"featureInterval\": 300000\n"
            + "      },\n"
            + "      {\n"
            + "        \"featureName\": \"byUserLocale\",\n"
            + "        \"dimensions\": [\n"
            + "          \"userLocale\"\n"
            + "        ],\n"
            + "        \"attributes\": [\n"
            + "          \"productPrice\",\n"
            + "          \"productQty\",\n"
            + "          \"productValue\"\n"
            + "        ],\n"
            + "        \"featureInterval\": 300000\n"
            + "      },\n"
            + "      {\n"
            + "        \"featureName\": \"byPrdBrandCategoryName\",\n"
            + "        \"dimensions\": [\n"
            + "          \"productBrand\",\n"
            + "          \"productCategory\",\n"
            + "          \"productName\"\n"
            + "        ],\n"
            + "        \"attributes\": [\n"
            + "          \"productPrice\",\n"
            + "          \"productQty\",\n"
            + "          \"productValue\"\n"
            + "        ],\n"
            + "        \"featureInterval\": 300000\n"
            + "      },\n"
            + "      {\n"
            + "        \"featureName\": \"byCPMNameMediumSource\",\n"
            + "        \"dimensions\": [\n"
            + "          \"campaignName\",\n"
            + "          \"campaignMedium\",\n"
            + "          \"campaignSource\"\n"
            + "        ],\n"
            + "        \"attributes\": [],\n"
            + "        \"featureInterval\": 300000\n"
            + "      }\n"
            + "    ]\n"
            + "  },\n"
            + "  \"dataSynthesisStatus\": \"Disabled\"\n"
            + "}\n";
    final var signal = mapper.readValue(signalSrc, SignalConfig.class);

    final String importSourceConfigSrc1 =
        "{"
            + "  'importSourceId': '1100',"
            + "  'importSourceName': 'Webhook Zerogrocery Segment',"
            + "  'type': 'Webhook',"
            + "  'webhookPath': '/segment'"
            + "}";
    final var importSourceConfig1 =
        mapper.readValue(importSourceConfigSrc1.replace("'", "\""), ImportSourceConfig.class);

    final String importSourceConfigSrc2 =
        "{"
            + "  'importSourceId': '1101',"
            + "  'importSourceName': 'Rest Client Segment',"
            + "  'type': 'RestClient',"
            + "  'method': 'GET'"
            + "}";
    final var importSourceConfig2 =
        mapper.readValue(importSourceConfigSrc2.replace("'", "\""), ImportSourceConfig.class);

    final String importDestinationConfigSrc =
        "{"
            + "  'importDestinationId': '2000',"
            + "  'type': 'BiosServer',"
            + "  'endpoint': 'https://bios.isima.io',"
            + "  'authentication': {"
            + "    'type': 'Login',"
            + "    'user': 'dummy@example.com',"
            + "    'password': 'whatever'"
            + "  }"
            + "}";
    final var importDestinationConfig =
        mapper.readValue(
            importDestinationConfigSrc.replace("'", "\""), ImportDestinationConfig.class);

    final String flowSpecSrc =
        "{\n"
            + "  `importFlowId`: `1501`,\n"
            + "  `importFlowName`: `Campaign Context Flow (type=page)`,\n"
            + "  `sourceDataSpec`: {\n"
            + "    `importSourceId`: `1100`,\n"
            + "    `payloadType`: `Json`\n"
            + "  },\n"
            + "  `destinationDataSpec`: {\n"
            + "    `importDestinationId`: `2000`,\n"
            + "    `type`: `Context`,\n"
            + "    `name`: `campaignContext`\n"
            + "  },\n"
            + "  `dataPickupSpec`: {\n"
            + "    `filters`: [\n"
            + "      {\n"
            + "        `sourceAttributeName`: `type`,\n"
            + "        `filter`: `lambda value: value == 'page'`\n"
            + "      }\n"
            + "    ],\n"
            + "    `attributeSearchPath`: ``,\n"
            + "    `attributes`: [\n"
            + "      {\n"
            + "        `sourceAttributeNames`: [\n"
            + "          `context/campaign/name`,\n"
            + "          `context/campaign/medium`,\n"
            + "          `context/campaign/source`,\n"
            + "          `context/campaign/term`,\n"
            + "          `context/campaign/content`\n"
            + "        ],\n"
            + "        `processes`: [\n"
            + "          {\n"
            + "            `processorName`: `hashUtils`,\n"
            + "            `method`: `md5_hash`\n"
            + "          }\n"
            + "        ],\n"
            + "        `as`: `campaignId`\n"
            + "      },\n"
            + "      {\n"
            + "        `sourceAttributeName`: `context/campaign/name`,\n"
            + "        `as`: `name`\n"
            + "      },\n"
            + "      {\n"
            + "        `sourceAttributeName`: `context/campaign/medium`,\n"
            + "        `as`: `medium`\n"
            + "      },\n"
            + "      {\n"
            + "        `sourceAttributeName`: `context/campaign/source`,\n"
            + "        `as`: `source`\n"
            + "      },\n"
            + "      {\n"
            + "        `sourceAttributeName`: `context/campaign/term`,\n"
            + "        `as`: `term`\n"
            + "      },\n"
            + "      {\n"
            + "        `sourceAttributeName`: `context/campaign/content`,\n"
            + "        `as`: `content`\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}\n";
    flowSpec = mapper.readValue(flowSpecSrc.replace("`", "\""), ImportFlowConfig.class);

    final String flowSpecSrc2 =
        "{\n"
            + "  `importFlowId`: `1601`,\n"
            + "  `importFlowName`: `Event Signal (type=page)`,\n"
            + "  `sourceDataSpec`: {\n"
            + "    `importSourceId`: `1100`,\n"
            + "    `payloadType`: `Json`\n"
            + "  },\n"
            + "  `destinationDataSpec`: {\n"
            + "    `importDestinationId`: `2000`,\n"
            + "    `type`: `Signal`,\n"
            + "    `name`: `eventSignal`\n"
            + "  },\n"
            + "  `dataPickupSpec`: {\n"
            + "    `filters`: [\n"
            + "      {\n"
            + "        `sourceAttributeName`: `type`,\n"
            + "        `filter`: `lambda value: value == 'page'`\n"
            + "      }\n"
            + "    ],\n"
            + "    `attributeSearchPath`: ``,\n"
            + "    `attributes`: [\n"
            + "      {\n"
            + "        `sourceAttributeName`: `type`\n"
            + "      },\n"
            + "      {\n"
            + "        `sourceAttributeName`: `event`\n"
            + "      },\n"
            + "      {\n"
            + "        `sourceAttributeName`: `channel`\n"
            + "      },\n"
            + "      {\n"
            + "        `sourceAttributeName`: `name`\n"
            + "      },\n"
            + "      {\n"
            + "        `sourceAttributeName`: `category`\n"
            + "      },\n"
            + "      {\n"
            + "        `sourceAttributeName`: `anonymousId`\n"
            + "      },\n"
            + "      {\n"
            + "        `sourceAttributeName`: `messageId`\n"
            + "      },\n"
            + "      {\n"
            + "        `sourceAttributeName`: `projectId`\n"
            + "      },\n"
            + "      {\n"
            + "        `sourceAttributeName`: `version`\n"
            + "      },\n"
            + "      {\n"
            + "        `sourceAttributeName`: `originalTimestamp`\n"
            + "      },\n"
            + "      {\n"
            + "        `sourceAttributeName`: `timestamp`\n"
            + "      },\n"
            + "      {\n"
            + "        `sourceAttributeName`: `receivedAt`\n"
            + "      },\n"
            + "      {\n"
            + "        `sourceAttributeName`: `sentAt`\n"
            + "      },\n"
            + "      {\n"
            + "        `sourceAttributeName`: `context/page/path`,\n"
            + "        `as`: `pagePath`\n"
            + "      },\n"
            + "      {\n"
            + "        `sourceAttributeName`: `context/page/referrer`,\n"
            + "        `as`: `pageReferrer`\n"
            + "      },\n"
            + "      {\n"
            + "        `sourceAttributeName`: `context/page/search`,\n"
            + "        `as`: `pageSearch`\n"
            + "      },\n"
            + "      {\n"
            + "        `sourceAttributeName`: `context/page/title`,\n"
            + "        `as`: `pageTitle`\n"
            + "      },\n"
            + "      {\n"
            + "        `sourceAttributeName`: `context/page/url`,\n"
            + "        `as`: `pageUrl`\n"
            + "      },\n"
            + "      {\n"
            + "        `sourceAttributeNames`: [\n"
            + "          `userId`,\n"
            + "          `anonymousId`\n"
            + "        ],\n"
            + "        `transforms`: [\n"
            + "          {\n"
            + "            `rule`: `lambda userId, anonymousId: userId or anonymousId`,\n"
            + "            `as`: `userId`\n"
            + "          }\n"
            + "        ]\n"
            + "      },\n"
            + "      {\n"
            + "        `sourceAttributeNames`: [\n"
            + "          `context/campaign/name`,\n"
            + "          `context/campaign/medium`,\n"
            + "          `context/campaign/source`,\n"
            + "          `context/campaign/term`,\n"
            + "          `context/campaign/content`\n"
            + "        ],\n"
            + "        `processes`: [\n"
            + "          {\n"
            + "            `processorName`: `hashUtils`,\n"
            + "            `method`: `md5_hash`\n"
            + "          }\n"
            + "        ],\n"
            + "        `as`: `campaignId`\n"
            + "      }\n"
            + "    ]\n"
            + "  },\n"
            + "  `acknowledgementEnabled`: true\n"
            + "}\n";
    flowSpec2 = mapper.readValue(flowSpecSrc2.replace("`", "\""), ImportFlowConfig.class);

    final String flowSpecSrc3 =
        "{\n"
            + "  \"importFlowId\": \"1602\",\n"
            + "  \"importFlowName\": \"Event Signal Rest (type=page)\",\n"
            + "  \"sourceDataSpec\": {\n"
            + "    \"importSourceId\": \"1101\",\n"
            + "    \"payloadType\": \"Json\",\n"
            + "    \"bodyParams\": {}\n"
            + "  },\n"
            + "  \"destinationDataSpec\": {\n"
            + "    \"importDestinationId\": \"2000\",\n"
            + "    \"type\": \"Signal\",\n"
            + "    \"name\": \"eventSignal\"\n"
            + "  },\n"
            + "  \"dataPickupSpec\": {\n"
            + "    \"attributeSearchPath\": \"\",\n"
            + "    \"attributes\": [\n"
            + "      {\n"
            + "        \"sourceAttributeName\": \"type\"\n"
            + "      },\n"
            + "      {\n"
            + "        \"sourceAttributeName\": \"event\"\n"
            + "      },\n"
            + "      {\n"
            + "        \"sourceAttributeName\": \"channel\"\n"
            + "      },\n"
            + "      {\n"
            + "        \"sourceAttributeName\": \"name\"\n"
            + "      },\n"
            + "      {\n"
            + "        \"sourceAttributeName\": \"category\"\n"
            + "      },\n"
            + "      {\n"
            + "        \"sourceAttributeName\": \"anonymousId\"\n"
            + "      },\n"
            + "      {\n"
            + "        \"sourceAttributeName\": \"messageId\"\n"
            + "      },\n"
            + "      {\n"
            + "        \"sourceAttributeName\": \"projectId\"\n"
            + "      },\n"
            + "      {\n"
            + "        \"sourceAttributeName\": \"version\"\n"
            + "      },\n"
            + "      {\n"
            + "        \"sourceAttributeName\": \"originalTimestamp\"\n"
            + "      },\n"
            + "      {\n"
            + "        \"sourceAttributeName\": \"timestamp\"\n"
            + "      },\n"
            + "      {\n"
            + "        \"sourceAttributeName\": \"receivedAt\"\n"
            + "      },\n"
            + "      {\n"
            + "        \"sourceAttributeName\": \"sentAt\"\n"
            + "      },\n"
            + "      {\n"
            + "        \"sourceAttributeName\": \"context/page/path\",\n"
            + "        \"as\": \"pagePath\"\n"
            + "      },\n"
            + "      {\n"
            + "        \"sourceAttributeName\": \"context/page/referrer\",\n"
            + "        \"as\": \"pageReferrer\"\n"
            + "      },\n"
            + "      {\n"
            + "        \"sourceAttributeName\": \"context/page/search\",\n"
            + "        \"as\": \"pageSearch\"\n"
            + "      },\n"
            + "      {\n"
            + "        \"sourceAttributeName\": \"context/page/title\",\n"
            + "        \"as\": \"pageTitle\"\n"
            + "      },\n"
            + "      {\n"
            + "        \"sourceAttributeName\": \"context/page/url\",\n"
            + "        \"as\": \"pageUrl\"\n"
            + "      }\n"
            + "    ]\n"
            + "  },\n"
            + "  \"acknowledgementEnabled\": true\n"
            + "}\n";
    flowSpec3 = mapper.readValue(flowSpecSrc3, ImportFlowConfig.class);

    tenantConfig = new TenantConfig("processesTest");
    tenantConfig.setSignals(List.of(signal));
    tenantConfig.setContexts(List.of(context));
    tenantConfig.setImportSources(List.of(importSourceConfig1, importSourceConfig2));
    tenantConfig.setImportDestinations(List.of(importDestinationConfig));
  }

  @Test
  public void testProcesses() throws Exception {
    // validator should pass against the initial tenant config
    ImportFlowSpecValidator.newValidator(tenantConfig, flowSpec).validate();
  }

  @Test
  public void testProcesses2() throws Exception {
    // validator should pass against the initial tenant config
    ImportFlowSpecValidator.newValidator(tenantConfig, flowSpec2).validate();
  }

  @Test
  public void testImportFlowNameMissing() {
    flowSpec.setImportFlowName(null);
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> ImportFlowSpecValidator.newValidator(tenantConfig, flowSpec).validate());
    assertThat(exception.getMessage(), is("importFlowSpec: importFlowName must be set"));
  }

  @Test
  public void testSourceDataSpecMissing() {
    flowSpec.setSourceDataSpec(null);
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> ImportFlowSpecValidator.newValidator(tenantConfig, flowSpec).validate());
    assertThat(
        exception.getMessage(), containsString("importFlowSpec: sourceDataSpec must be set"));
  }

  @Test
  public void testSourceIdMissing() {
    flowSpec.getSourceDataSpec().setImportSourceId(null);
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> ImportFlowSpecValidator.newValidator(tenantConfig, flowSpec).validate());
    assertThat(
        exception.getMessage(),
        containsString("importFlowSpec.sourceDataSpec: importSourceId must be set"));
  }

  @Test
  public void testSourceIdMismatch() {
    flowSpec.getSourceDataSpec().setImportSourceId("no such id");
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> ImportFlowSpecValidator.newValidator(tenantConfig, flowSpec).validate());
    assertThat(
        exception.getMessage(),
        containsString(
            "importFlowSpec.sourceDataSpec: importSourceId 'no such id' not found"
                + " in existing importSources"));
  }

  @Test
  public void testSourcePayloadTypeMissing() {
    flowSpec.getSourceDataSpec().setPayloadType(null);
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> ImportFlowSpecValidator.newValidator(tenantConfig, flowSpec).validate());
    assertThat(
        exception.getMessage(),
        containsString("importFlowSpec.sourceDataSpec: payloadType must be set"));
  }

  @Test
  public void testDestinationDataSpecMissing() {
    flowSpec.setDestinationDataSpec(null);
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> ImportFlowSpecValidator.newValidator(tenantConfig, flowSpec).validate());
    assertThat(
        exception.getMessage(), containsString("importFlowSpec: destinationDataSpec must be set"));
  }

  @Test
  public void testDestinationIdMissing() {
    flowSpec.getDestinationDataSpec().setImportDestinationId(null);
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> ImportFlowSpecValidator.newValidator(tenantConfig, flowSpec).validate());
    assertThat(
        exception.getMessage(),
        containsString("importFlowSpec.destinationDataSpec: importDestinationId must be set"));
  }

  @Test
  public void testDestinationIdMissMatch() {
    flowSpec.getDestinationDataSpec().setImportDestinationId("badId");
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> ImportFlowSpecValidator.newValidator(tenantConfig, flowSpec).validate());
    assertThat(
        exception.getMessage(),
        containsString(
            "importFlowSpec.destinationDataSpec: importDestinationId 'badId' not found"
                + " in existing importDestinations"));
  }

  @Test
  public void testDestinationTypeMissing() {
    flowSpec.getDestinationDataSpec().setType(null);
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> ImportFlowSpecValidator.newValidator(tenantConfig, flowSpec).validate());
    assertThat(
        exception.getMessage(),
        containsString("importFlowSpec.destinationDataSpec: property 'type' must be set"));
  }

  @Test
  public void testDestinationStreamNameMissing() {
    flowSpec.getDestinationDataSpec().setName(null);
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> ImportFlowSpecValidator.newValidator(tenantConfig, flowSpec).validate());
    assertThat(exception.getMessage(), containsString("Property 'name' must be set"));
  }

  @Test
  public void testDestinationStreamNameMismatch() {
    flowSpec.getDestinationDataSpec().setName("nonExistingContext");
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> ImportFlowSpecValidator.newValidator(tenantConfig, flowSpec).validate());
    assertThat(
        exception.getMessage(),
        containsString(
            "importFlowSpec.destinationDataSpec: Target context nonExistingContext is missing"));
  }

  @Test
  public void testDataPickupSpecMissing() {
    flowSpec.setDataPickupSpec(null);
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> ImportFlowSpecValidator.newValidator(tenantConfig, flowSpec).validate());
    assertThat(
        exception.getMessage(), containsString("importFlowSpec: dataPickupSpec must be set"));
  }

  @Test
  public void testNameAndNamesMissing() {
    flowSpec.getDataPickupSpec().getAttributes().get(3).setSourceAttributeName(null);
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> ImportFlowSpecValidator.newValidator(tenantConfig, flowSpec).validate());
    assertThat(
        exception.getMessage(),
        containsString(
            "importFlowSpec.dataPickupSpec.attributes[3]:"
                + " Property sourceAttributeName or sourceAttributeNames must be set"));
  }

  @Test
  public void testNameAndNamesSet() {
    flowSpec.getDataPickupSpec().getAttributes().get(0).setSourceAttributeName("bad");
    final var exception =
        assertThrows(
            MultipleValidatorViolationsException.class,
            () -> ImportFlowSpecValidator.newValidator(tenantConfig, flowSpec).validate());
    assertThat(
        exception.getMessage(),
        containsString(
            "importFlowSpec.dataPickupSpec.attributes[0]:"
                + " Only one of 'sourceAttributeName' or 'sourceAttributeNames' can be set"));
  }

  @Test
  public void testMissingNecessaryOutputName() {
    flowSpec.getDataPickupSpec().getAttributes().get(1).setAs(null);
    final var exception =
        assertThrows(
            MultipleValidatorViolationsException.class,
            () -> ImportFlowSpecValidator.newValidator(tenantConfig, flowSpec).validate());
    assertEquals(2, exception.getErrors().size());
  }

  @Test
  public void testTypoInOutputName() {
    flowSpec.getDataPickupSpec().getAttributes().get(1).setAs("wrong");
    final var exception =
        assertThrows(
            MultipleValidatorViolationsException.class,
            () -> ImportFlowSpecValidator.newValidator(tenantConfig, flowSpec).validate());
    assertEquals(2, exception.getErrors().size());
  }

  @Test
  public void testDuplicateOutputName() {
    flowSpec.getDataPickupSpec().getAttributes().get(3).setAs("term");
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> ImportFlowSpecValidator.newValidator(tenantConfig, flowSpec).validate());
    assertThat(
        exception.getMessage(),
        containsString(
            "importFlowSpec.dataPickupSpec.attributes[4]: Duplicate destination attribute name: term"));
  }

  @Test
  public void testMultipleSourceNamesMissingOutputName() {
    flowSpec.getDataPickupSpec().getAttributes().get(0).setAs(null);
    final var exception =
        assertThrows(
            MultipleValidatorViolationsException.class,
            () -> ImportFlowSpecValidator.newValidator(tenantConfig, flowSpec).validate());
    assertThat(
        exception.getMessage(),
        containsString("importFlowSpec.dataPickupSpec.attributes[0]: Property 'as' must be set"));
    assertThat(
        exception.getMessage(),
        containsString(
            "importFlowSpec: Attribute 'campaignId' of context 'campaignContext' is not"
                + " covered by the flow, missingAttributePolicy must be 'StoreDefaultValue'"
                + " but is 'Reject'"));
    System.out.println(exception);
  }

  @Test
  public void testEmptyTransforms() {
    flowSpec2.getDataPickupSpec().getAttributes().get(18).setTransforms(List.of());
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> ImportFlowSpecValidator.newValidator(tenantConfig, flowSpec2).validate());
  }

  @Test
  public void testTransformRuleMissing() {
    flowSpec2.getDataPickupSpec().getAttributes().get(18).getTransforms().get(0).setRule(null);
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> ImportFlowSpecValidator.newValidator(tenantConfig, flowSpec2).validate());
    assertThat(exception.getMessage(), containsString("Property 'rule' must be set"));
  }

  @Test
  public void testTransformOutputNameMissing() {
    flowSpec2.getDataPickupSpec().getAttributes().get(18).getTransforms().get(0).setAs(null);
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> ImportFlowSpecValidator.newValidator(tenantConfig, flowSpec2).validate());
    assertThat(exception.getMessage(), containsString("Property 'as' must be set"));
  }

  @Test
  public void testTransformOutputNameConflict() {
    flowSpec2
        .getDataPickupSpec()
        .getAttributes()
        .get(18)
        .getTransforms()
        .get(0)
        .setAs("campaignid");
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> ImportFlowSpecValidator.newValidator(tenantConfig, flowSpec2).validate());
    assertThat(
        exception.getMessage(),
        containsString(
            "importFlowSpec.dataPickupSpec.attributes[19]:"
                + " Duplicate destination attribute name: campaignId"));
  }

  @Test
  public void testTransformOutputNameConflict2() {
    flowSpec2.getDataPickupSpec().getAttributes().get(18).getTransforms().get(0).setAs("pageurl");
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> ImportFlowSpecValidator.newValidator(tenantConfig, flowSpec2).validate());
    assertThat(
        exception.getMessage(),
        containsString(
            "importFlowSpec.dataPickupSpec.attributes[18].transforms[0]:"
                + " Duplicate destination attribute name: pageurl"));
  }

  @Test
  public void testTransformOutputNameMismatch() {
    flowSpec2.getDataPickupSpec().getAttributes().get(18).getTransforms().get(0).setAs("aaaa");
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> ImportFlowSpecValidator.newValidator(tenantConfig, flowSpec2).validate());
    assertThat(
        exception.getMessage(),
        containsString(
            "importFlowSpec.dataPickupSpec.attributes[18].transforms[0]:"
                + " Destination attribute 'aaaa' is missing in the attributes of"
                + " signal 'eventSignal'"));
  }

  @Test
  public void testRestMethodAndBodyParamsMismatch() {
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> ImportFlowSpecValidator.newValidator(tenantConfig, flowSpec3).validate());
    assertThat(
        exception.getMessage(),
        containsString("bodyParams must be not be set for http GET method"));
  }

  @Test
  public void testValidDeletionSpec() throws Exception {
    final var deletionSpec = new DataPickupSpec.DeletionSpec();
    deletionSpec.setSourceAttributeName("deletionFlag");
    deletionSpec.setCondition("lambda flag: flag == 'DELETE'");
    flowSpec.getDataPickupSpec().setDeletionSpec(deletionSpec);
    ImportFlowSpecValidator.newValidator(tenantConfig, flowSpec).validate();
  }

  @Test
  public void testDeletionSpecSetToSignalFlow() {
    final var deletionSpec = new DataPickupSpec.DeletionSpec();
    deletionSpec.setSourceAttributeName("deletionFlag");
    deletionSpec.setCondition("lambda flag: flag == 'DELETE'");
    flowSpec2.getDataPickupSpec().setDeletionSpec(deletionSpec);
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> ImportFlowSpecValidator.newValidator(tenantConfig, flowSpec2).validate());
    assertThat(
        exception.getMessage(),
        containsString(
            "importFlowSpec: deletionSpec may not be set for a destination of type Signal;"));
  }
}
