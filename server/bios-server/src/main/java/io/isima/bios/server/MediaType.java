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
package io.isima.bios.server;

import io.isima.bios.server.codec.JsonPayloadCodec;
import io.isima.bios.server.codec.PayloadCodec;
import io.isima.bios.server.codec.ProtobufPayloadCodec;
import io.isima.bios.server.codec.TextPayloadCodec;
import io.netty.handler.codec.http.HttpHeaderValues;

public enum MediaType {
  APPLICATION_JSON(
      HttpHeaderValues.APPLICATION_JSON.toString(), new JsonPayloadCodec().enableValidation()),
  APPLICATION_JSON_NO_VALIDATION(
      HttpHeaderValues.APPLICATION_JSON.toString(), new JsonPayloadCodec()),
  APPLICATION_JSON_DECODE_TO_STRING(
      HttpHeaderValues.APPLICATION_JSON.toString(), getTextPayloadCodec()),
  X_PROTOBUF("application/x-protobuf", new ProtobufPayloadCodec()),
  TEXT_PLAIN(HttpHeaderValues.TEXT_PLAIN.toString(), getTextPayloadCodec()),
  MULTIPART_FORM_DATA(
      // We don't parse the body in the I/O thread to prevent the I/O thread from spending
      // too much CPU time. We'll do it in a sideline.
      HttpHeaderValues.MULTIPART_FORM_DATA.toString(), getTextPayloadCodec()),
  NONE("", null);

  private static TextPayloadCodec textPayloadCodec;

  private static TextPayloadCodec getTextPayloadCodec() {
    if (textPayloadCodec == null) {
      textPayloadCodec = new TextPayloadCodec();
    }
    return textPayloadCodec;
  }

  private String valueString;
  private PayloadCodec payloadCodec;

  private MediaType(String value, PayloadCodec payloadCodec) {
    this.valueString = value;
    this.payloadCodec = payloadCodec;
  }

  public String value() {
    return valueString;
  }

  public PayloadCodec payloadCodec() {
    return payloadCodec;
  }

  public static MediaType resolve(String src) {
    final var test = src.trim();
    for (var entry : values()) {
      if (entry.value().equalsIgnoreCase(test)) {
        return entry;
      }
    }
    return null;
  }
}
