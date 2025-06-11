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
import io.isima.bios.models.MissingAttributePolicy;
import io.isima.bios.models.SignalConfig;
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

public class SignalOpsIT {
  private static final String TENANT_NAME = "biosClientE2eTest";

  private SignalConfig signalConfig;

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
    signalConfig = new SignalConfig();
    signalConfig.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
    signalConfig.setAttributes(
        List.of(
            new AttributeConfig("one", AttributeType.STRING),
            new AttributeConfig("two", AttributeType.INTEGER)));
  }

  @Test
  public void signalCrud() throws Exception {
    final String signalName = "signalCrudTest";
    try (final Session session =
        Bios.newSession(host)
            .port(port)
            .user(admin_user + "@" + TENANT_NAME)
            .password(admin_pass)
            .connect()) {

      try {
        signalConfig.setName(signalName);
        final var created = session.createSignal(signalConfig);
        assertThat(created, notNullValue());

        {
          final var retrieved = session.getSignal(signalName, List.of());
          assertThat(retrieved, equalTo(created));
        }

        {
          final var exception =
              assertThrows(
                  BiosClientException.class, () -> session.updateSignal(signalName, signalConfig));
          assertThat(exception.getCode(), is(BiosClientError.BAD_INPUT));
        }

        final var newAttributes = new ArrayList<>(signalConfig.getAttributes());
        final var thirdAttribute = new AttributeConfig("three", AttributeType.DECIMAL);
        thirdAttribute.setDefaultValue(new AttributeValueGeneric(1.23, AttributeType.DECIMAL));
        newAttributes.add(thirdAttribute);
        signalConfig.setAttributes(newAttributes);
        final var modified = session.updateSignal(signalName, signalConfig);
        assertThat(modified, notNullValue());
        assertThat(modified.getAttributes(), equalTo(signalConfig.getAttributes()));

        {
          final var retrieved2 = session.getSignal(signalName, List.of());
          assertThat(retrieved2, equalTo(modified));
        }

        session.deleteSignal(signalName);

        {
          final var exception =
              assertThrows(
                  BiosClientException.class, () -> session.getSignal(signalName, List.of()));
          assertThat(exception.getCode(), is(BiosClientError.NO_SUCH_STREAM));
        }

        signalConfig = null;
      } finally {
        if (signalConfig != null) {
          session.deleteSignal(signalName);
        }
      }
    }
  }

  @Test
  public void listSignals() throws Exception {
    final String signalName = "signalListTest";
    try (final Session session =
        Bios.newSession(host)
            .port(port)
            .user(admin_user + "@" + TENANT_NAME)
            .password(admin_pass)
            .connect()) {

      try {
        signalConfig.setName(signalName);
        session.createSignal(signalConfig);

        int numRegularSignals;
        {
          final var signals = session.getSignals(List.of(), List.of());
          assertThat(signals, notNullValue());
          numRegularSignals = signals.size();
          assertThat(numRegularSignals, greaterThanOrEqualTo(1));
          final var retrievedSignal =
              signals.stream().filter((signal) -> signal.getName().equals(signalName)).findAny();
          assertThat(retrievedSignal.isPresent(), is(true));
          assertThat(retrievedSignal.get().getAttributes(), nullValue());
        }
        {
          final var signals = session.getSignals(List.of(), List.of(GetStreamOption.DETAIL));
          assertThat(signals, notNullValue());
          assertThat(signals.size(), is(numRegularSignals));
          final var retrievedSignal =
              signals.stream().filter((signal) -> signal.getName().equals(signalName)).findAny();
          assertThat(retrievedSignal.isPresent(), is(true));
          assertThat(retrievedSignal.get().getAttributes(), notNullValue());
          assertThat(retrievedSignal.get().getAttributes().get(0).getInferredTags(), nullValue());
        }
        {
          final var signals =
              session.getSignals(List.of(), List.of(GetStreamOption.INCLUDE_INTERNAL));
          assertThat(signals.size(), greaterThan(numRegularSignals));
          final var usageSignal =
              signals.stream().filter((signal) -> signal.getName().equals("_usage")).findAny();
          assertThat(usageSignal.isPresent(), is(true));
          assertThat(usageSignal.get().getAttributes(), nullValue());
        }
        {
          final var signals =
              session.getSignals(
                  List.of(), List.of(GetStreamOption.DETAIL, GetStreamOption.INCLUDE_INTERNAL));
          assertThat(signals.size(), greaterThan(numRegularSignals));
          final var usageSignal =
              signals.stream().filter((signal) -> signal.getName().equals("_usage")).findAny();
          assertThat(usageSignal.isPresent(), is(true));
          assertThat(usageSignal.get().getAttributes(), notNullValue());
        }
      } finally {
        session.deleteSignal(signalName);
      }
    }
  }
}
