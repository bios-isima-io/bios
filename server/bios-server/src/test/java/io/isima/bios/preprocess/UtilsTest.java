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
package io.isima.bios.preprocess;

import static org.junit.Assert.fail;

import com.datastax.driver.core.utils.UUIDs;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.errors.exception.InvalidValueException;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import java.util.Date;
import org.junit.Test;

public class UtilsTest {

  @Test
  public void testValidateEventBasic() throws InvalidValueException {
    final StreamDesc streamDesc =
        new StreamDesc("testValidateEventBasic", System.currentTimeMillis(), false);
    streamDesc.addAttribute(new AttributeDesc("hello", InternalAttributeType.STRING));
    streamDesc.addAttribute(new AttributeDesc("world", InternalAttributeType.LONG));

    final Event event1 = new EventJson();
    event1.setEventId(UUIDs.timeBased());
    event1.setIngestTimestamp(new Date(System.currentTimeMillis()));

    expectInvalid(streamDesc, event1);

    event1.set("hello", "abc");
    expectInvalid(streamDesc, event1);

    event1.set("world", Long.valueOf(0));
    Utils.validateEvent(streamDesc, event1);

    event1.set("hello", null);
    expectInvalid(streamDesc, event1);
  }

  @Test
  public void testValidateEventOnlyAdditional() throws InvalidValueException {
    final StreamDesc streamDesc =
        new StreamDesc("testValidateEventBasic", System.currentTimeMillis(), false);
    streamDesc.addAdditionalAttribute(new AttributeDesc("hello", InternalAttributeType.STRING));
    streamDesc.addAdditionalAttribute(new AttributeDesc("world", InternalAttributeType.LONG));

    final Event event1 = new EventJson();
    event1.setEventId(UUIDs.timeBased());
    event1.setIngestTimestamp(new Date(System.currentTimeMillis()));

    expectInvalid(streamDesc, event1);

    event1.set("hello", "abc");
    expectInvalid(streamDesc, event1);

    event1.set("world", Long.valueOf(0));
    Utils.validateEvent(streamDesc, event1);
  }

  @Test
  public void testValidateEventFull() throws InvalidValueException {
    final StreamDesc streamDesc =
        new StreamDesc("testValidateEventBasic", System.currentTimeMillis(), false);
    streamDesc.addAttribute(new AttributeDesc("hello", InternalAttributeType.STRING));
    streamDesc.addAdditionalAttribute(new AttributeDesc("world", InternalAttributeType.LONG));

    final Event event1 = new EventJson();
    event1.setEventId(UUIDs.timeBased());
    event1.setIngestTimestamp(new Date(System.currentTimeMillis()));

    expectInvalid(streamDesc, event1);

    event1.set("hello", "abc");
    expectInvalid(streamDesc, event1);

    event1.set("world", Long.valueOf(0));
    Utils.validateEvent(streamDesc, event1);

    event1.set("hello", null);
    expectInvalid(streamDesc, event1);

    event1.set("hello", "def");
    Utils.validateEvent(streamDesc, event1);

    event1.set("world", null);
    expectInvalid(streamDesc, event1);
  }

  private void expectInvalid(StreamDesc streamDesc, Event event) {
    try {
      Utils.validateEvent(streamDesc, event);
      fail("Exception is expected");
    } catch (InvalidValueException e) {
      // ok
    }
  }
}
