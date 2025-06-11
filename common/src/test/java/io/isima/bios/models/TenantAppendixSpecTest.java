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
package io.isima.bios.models;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.utils.BiosObjectMapperProvider;
import org.junit.BeforeClass;
import org.junit.Test;

public class TenantAppendixSpecTest {
  private static ObjectMapper objectMapper;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    objectMapper = BiosObjectMapperProvider.get();
  }

  @Test
  public void jsonCodec() throws Exception {
    final var src =
        "{"
            + "  '@type': 'IMPORT_SOURCES',"
            + "  'entryId': 'webhookImportSourceId',"
            + "  'content': {"
            + "    'importSourceName': 'webhookSource',"
            + "    'type': 'Webhook',"
            + "    'webhookPath': '/alerts'"
            + "  }"
            + "}";

    // test decoding
    final TenantAppendixSpec<?> spec =
        objectMapper.readValue(src.replace("'", "\""), TenantAppendixSpec.class);
    System.out.println(spec);
    assertThat(spec.getEntryId(), is("webhookImportSourceId"));
    assertThat(spec.getContent(), instanceOf(ImportSourceConfig.class));
    final var importSourceConfig = (ImportSourceConfig) spec.getContent();
    assertThat(importSourceConfig.getImportSourceName(), is("webhookSource"));
    assertThat(importSourceConfig.getType(), is(ImportSourceType.WEBHOOK));
    assertThat(importSourceConfig.getWebhookPath(), is("/alerts"));

    // test encoding
    final var out = objectMapper.writeValueAsString(spec);
    final var expected = src.replace("'", "\"").replace(" ", "").replace("\n", "");
    assertThat(out, is(expected));
  }
}
