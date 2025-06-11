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
package io.isima.bios.alerts;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.models.AlertConfig;
import io.isima.bios.models.AlertType;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.models.v1.AlertNotification;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RollupAlertNotificationTest {

  @Before
  public void setUp() {}

  @After
  public void tearDown() {}

  @Test
  public void testSerialization() throws JsonProcessingException, IOException {
    EventJson event = new EventJson();
    event.setEventId(new UUID(1, 2));
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("item_id", "2312312");
    attributes.put("item_price", 100);
    attributes.put("quantity_sold", 10);
    event.setAttributes(attributes);
    AlertConfig alert =
        new AlertConfig(
            "item_sold_at_high_price_alert",
            AlertType.JSON,
            "(max(item_price) > 80)",
            "https://webhook.site/99743393-3a47-473f-8676-319e8c5d9422",
            "testUser",
            "testPassword");
    AlertNotification alertNotification = new AlertNotification(alert, event);
    ObjectMapper mapper = TfosObjectMapperProvider.get();
    String alertJson = mapper.writeValueAsString(alertNotification);

    AlertNotification deserializedAlertNotification =
        mapper.readValue(alertJson, AlertNotification.class);

    AlertConfig deserializedAlert = deserializedAlertNotification.getAlert();
    Event deserializedEvent = deserializedAlertNotification.getEvent();

    Assert.assertEquals("item_sold_at_high_price_alert", deserializedAlert.getName());
    Assert.assertEquals(
        "https://webhook.site/99743393-3a47-473f-8676-319e8c5d9422",
        deserializedAlert.getWebhookUrl());
    Assert.assertEquals("(max(item_price) > 80)", deserializedAlert.getCondition());
    Assert.assertEquals("testUser", deserializedAlert.getUserName());
    Assert.assertEquals("testPassword", deserializedAlert.getPassword());

    Map<String, Object> deserializedAttributes = deserializedEvent.getAttributes();
    Assert.assertEquals("2312312", deserializedAttributes.get("item_id"));
    Assert.assertEquals(100, deserializedAttributes.get("item_price"));
    Assert.assertEquals(10, deserializedAttributes.get("quantity_sold"));
  }

  @Test
  public void testEqualsAndHash() throws Exception {
    final var alertA =
        new AlertConfig(
            "item_sold_at_high_price_alert",
            AlertType.JSON,
            "(max(item_price) > 80)",
            "https://webhook.site/99743393-3a47-473f-8676-319e8c5d9422",
            "rajinikanth@isima.io",
            "Test123!");

    final var alertB =
        new AlertConfig(
            "item_sold_at_high_price_alert",
            AlertType.JSON,
            "(max(item_price) > 80)",
            "https://webhook.site/99743393-3a47-473f-8676-319e8c5d9422",
            "rajinikanth@isima.io",
            "Test123!");

    // Test equality
    assertEquals(alertB, alertA);
    assertEquals(alertB.hashCode(), alertB.hashCode());
    final var clone = alertA.duplicate();
    assertNotSame(clone, alertA);
    assertEquals(clone, alertA);
    assertEquals(clone.hashCode(), alertA.hashCode());
    alertB.setUserName(null);
    clone.setUserName(null);
    assertEquals(clone, alertB);
    alertB.setPassword(null);
    clone.setPassword(null);
    assertModNotEquals(alertA, "setName", "abc");
    assertModNotEquals(alertA, "setCondition", "abc");
    assertModNotEquals(alertA, "setUserName", "abc");
    assertModNotEquals(alertA, "setUserName", null);
    assertModNotEquals(alertA, "setPassword", "abc");
    assertModNotEquals(alertA, "setPassword", null);

    // Check case insensitivity
    assertModEquals(alertA, "setName", alertA.getName().toUpperCase());
    assertModNotEquals(alertA, "setCondition", alertA.getCondition().toUpperCase());
    assertModEquals(alertA, "setWebhookUrl", alertA.getWebhookUrl().toUpperCase());
    assertModEquals(alertA, "setUserName", alertA.getUserName().toUpperCase());
    assertModNotEquals(alertA, "setPassword", alertA.getPassword().toUpperCase());
  }

  private void assertModEquals(AlertConfig orig, String modifierName, String newValue)
      throws Exception {
    final var clone = cloneAndMod(orig, modifierName, newValue);
    assertEquals(clone, orig);
    assertEquals(orig, clone);
    assertEquals(clone.hashCode(), orig.hashCode());
  }

  private void assertModNotEquals(AlertConfig orig, String modifierName, String newValue)
      throws Exception {
    final var clone = cloneAndMod(orig, modifierName, newValue);
    assertNotEquals(clone, orig);
    assertNotEquals(orig, clone);
    assertNotEquals(clone.hashCode(), orig.hashCode());
  }

  private AlertConfig cloneAndMod(AlertConfig orig, String modifierName, String newValue)
      throws Exception {
    final var clone = orig.duplicate();
    final var modifier = clone.getClass().getMethod(modifierName, String.class);
    modifier.invoke(clone, newValue);
    return clone;
  }
}
