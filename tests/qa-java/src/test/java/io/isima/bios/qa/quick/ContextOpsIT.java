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
package io.isima.bios.qa.quick;

import static io.isima.bios.qautils.TestConfig.admin_pass;
import static io.isima.bios.qautils.TestConfig.admin_user;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assume.assumeTrue;

import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.AttributeValueGeneric;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.MissingAttributePolicy;
import io.isima.bios.qautils.TestConfig;
import io.isima.bios.sdk.Bios;
import io.isima.bios.sdk.GetStreamOption;
import io.isima.bios.sdk.Session;
import io.isima.bios.sdk.errors.BiosClientError;
import io.isima.bios.sdk.exceptions.BiosClientException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ContextOpsIT {
  private static final String TENANT_NAME = "biosClientE2eTest";

  private ContextConfig contextConfig;

  private static String host;
  private static int port;

  @BeforeClass
  public static void setUpBeforeClass() {
    assumeTrue(TestConfig.isIntegrationTest());
    host = TestConfig.getEndpoint();
    port = TestConfig.getPort();
  }

  @Before
  public void setUp() {
    contextConfig = new ContextConfig();
    contextConfig.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
    contextConfig.setAttributes(
        List.of(
            new AttributeConfig("one", AttributeType.STRING),
            new AttributeConfig("two", AttributeType.INTEGER)));
    contextConfig.setPrimaryKey(List.of("one"));
    contextConfig.setAuditEnabled(true);
  }

  @Test
  public void contextCrud() throws Exception {
    final String contextName = "contextCrudTest";
    try (final Session session =
        Bios.newSession(host)
            .port(port)
            .user(admin_user + "@" + TENANT_NAME)
            .password(admin_pass)
            .connect()) {

      try {
        contextConfig.setName(contextName);
        final var created = session.createContext(contextConfig);
        assertThat(created, notNullValue());

        {
          final var retrieved = session.getContext(contextName, List.of());
          assertThat(retrieved, equalTo(created));
        }

        {
          final var exception =
              assertThrows(
                  BiosClientException.class,
                  () -> session.updateContext(contextName, contextConfig));
          assertThat(exception.getCode(), is(BiosClientError.BAD_INPUT));
        }

        final var newAttributes = new ArrayList<>(contextConfig.getAttributes());
        final var thirdAttribute = new AttributeConfig("three", AttributeType.DECIMAL);
        thirdAttribute.setDefaultValue(new AttributeValueGeneric(1.23, AttributeType.DECIMAL));
        newAttributes.add(thirdAttribute);
        contextConfig.setAttributes(newAttributes);
        final var modified = session.updateContext(contextName, contextConfig);
        assertThat(modified, notNullValue());
        assertThat(modified.getAttributes(), equalTo(contextConfig.getAttributes()));

        {
          final var retrieved2 = session.getContext(contextName, List.of());
          assertThat(retrieved2, equalTo(modified));
        }

        session.deleteContext(contextName);

        {
          final var exception =
              assertThrows(
                  BiosClientException.class, () -> session.getContext(contextName, List.of()));
          assertThat(exception.getCode(), is(BiosClientError.NO_SUCH_STREAM));
        }

        contextConfig = null;
      } finally {
        if (contextConfig != null) {
          session.deleteContext(contextName);
        }
      }
    }
  }

  @Test
  public void listContexts() throws Exception {
    final String contextName = "contextListTest";
    try (final Session session =
        Bios.newSession(host)
            .port(port)
            .user(admin_user + "@" + TENANT_NAME)
            .password(admin_pass)
            .connect()) {

      try {
        contextConfig.setName(contextName);
        session.createContext(contextConfig);

        int numRegularContexts;
        {
          final var contexts = session.getContexts(List.of(), List.of());
          assertThat(contexts, notNullValue());
          numRegularContexts = contexts.size();
          assertThat(numRegularContexts, greaterThanOrEqualTo(1));
          final var retrievedContext =
              contexts.stream()
                  .filter((context) -> context.getName().equals(contextName))
                  .findAny();
          assertThat(retrievedContext.isPresent(), is(true));
          assertThat(retrievedContext.get().getAttributes(), nullValue());
        }
        {
          final var contexts = session.getContexts(List.of(), List.of(GetStreamOption.DETAIL));
          assertThat(contexts, notNullValue());
          assertThat(contexts.size(), is(numRegularContexts));
          final var retrievedContext =
              contexts.stream()
                  .filter((context) -> context.getName().equals(contextName))
                  .findAny();
          assertThat(retrievedContext.isPresent(), is(true));
          assertThat(retrievedContext.get().getAttributes(), notNullValue());
          assertThat(retrievedContext.get().getAttributes().get(0).getInferredTags(), nullValue());
        }
        {
          final var contexts =
              session.getContexts(List.of(), List.of(GetStreamOption.INCLUDE_INTERNAL));
          assertThat(contexts.size(), greaterThan(numRegularContexts));
          final var ip2geoContext =
              contexts.stream().filter((context) -> context.getName().equals("_ip2geo")).findAny();
          assertThat(ip2geoContext.isPresent(), is(true));
          assertThat(ip2geoContext.get().getAttributes(), nullValue());
        }
        {
          final var contexts =
              session.getContexts(
                  List.of(), List.of(GetStreamOption.DETAIL, GetStreamOption.INCLUDE_INTERNAL));
          assertThat(contexts.size(), greaterThan(numRegularContexts));
          final var ip2geoContext =
              contexts.stream().filter((context) -> context.getName().equals("_ip2geo")).findAny();
          assertThat(ip2geoContext.isPresent(), is(true));
          assertThat(ip2geoContext.get().getAttributes(), notNullValue());
        }
      } finally {
        session.deleteContext(contextName);
      }
    }
  }
}
