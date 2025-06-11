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
package io.isima.bios.data.synthesis;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.AttributeValueGeneric;
import io.isima.bios.models.Event;
import io.isima.bios.models.SignalConfig;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import org.junit.Test;

public class RecordGeneratorTest {

  @Test
  public void testFundamental() throws Exception {
    final var signalConfig = new SignalConfig();
    signalConfig.setName("testtest");
    signalConfig.setVersion(System.currentTimeMillis());

    final var attributes = new ArrayList<AttributeConfig>();
    attributes.add(new AttributeConfig("productId", AttributeType.INTEGER));
    attributes.add(new AttributeConfig("productName", AttributeType.STRING));
    final var enumAttribute = new AttributeConfig("productType", AttributeType.STRING);
    enumAttribute.setAllowedValues(
        List.of("DENTAL", "EYE", "FIRST_AID", "HARDWARE").stream()
            .map((value) -> new AttributeValueGeneric(value, AttributeType.STRING))
            .collect(Collectors.toList()));
    attributes.add(enumAttribute);
    attributes.add(new AttributeConfig("delivered", AttributeType.BOOLEAN));
    signalConfig.setAttributes(attributes);

    final List<Event> events = new ArrayList<>();

    final TestSynthesizer synthesizer =
        new TestSynthesizer(
            events, "tenant", signalConfig, Executors.newScheduledThreadPool(1), 10.0);

    final long toStop = System.currentTimeMillis() + 5000;

    final Event event = synthesizer.generate();
    assertThat(event, instanceOf(Event.class));
    assertNotNull(event.getEventId());
    assertNotNull(event.getIngestTimestamp());
    assertEquals(4, event.getAttributes().size());
    assertThat(event.get("productId"), instanceOf(Long.class));
    assertThat(event.get("productName"), instanceOf(String.class));
    assertThat(event.get("productType"), instanceOf(Integer.class));
    assertThat(event.get("delivered"), instanceOf(Boolean.class));

    Thread.sleep(toStop - System.currentTimeMillis());

    synthesizer.stop();
    Thread.sleep(100);
    int size1 = events.size();
    // allow 15% of frequency errors
    assertThat(size1, greaterThan(42));
    assertThat(size1, lessThan(58));

    final Set<UUID> uuids = new HashSet<>();
    events.forEach(
        (entry) -> {
          assertThat(entry, instanceOf(Event.class));
          assertFalse(uuids.contains(entry.getEventId()));
          uuids.add(entry.getEventId());
        });

    Thread.sleep(100);
    assertThat(events.size(), is(size1));
  }

  private static class TestSynthesizer extends SignalSynthesizerBase {
    private List<Event> events;

    public TestSynthesizer(
        List<Event> events,
        String tenantName,
        SignalConfig signalConfig,
        ScheduledExecutorService scheduler,
        double publishesPerSecond)
        throws TfosException, ApplicationException {
      super(tenantName, signalConfig, null, null, scheduler, publishesPerSecond);
      this.events = events;
    }

    @Override
    protected void publish() {
      events.add(generator.generate());
    }
  }
}
