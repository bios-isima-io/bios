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
package io.isima.bios.dto;

import com.google.common.net.InetAddresses;
import io.isima.bios.errors.exception.NoSuchAttributeException;
import io.isima.bios.errors.exception.TypeMismatchException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

public interface Data {
  UUID getEventId();

  Date getTimestamp();

  Map<String, Object> getAttributes();

  Object get(String name) throws NoSuchAttributeException;

  /**
   * Get attribute as a String.
   *
   * @param name Attribute Name.
   * @return Attribute value as a String.
   */
  default String getAsString(String name) throws NoSuchAttributeException, TypeMismatchException {
    if (!getAttributes().containsKey(name)) {
      throw new NoSuchAttributeException(String.format("attribute %s not found", name));
    }
    return validateSrc(name);
  }

  default int getAsInt(String name) throws NoSuchAttributeException, TypeMismatchException {
    if (!getAttributes().containsKey(name)) {
      throw new NoSuchAttributeException(String.format("attribute %s not found", name));
    }
    final String normalized = validateSrc(name);
    try {
      return Integer.parseInt(normalized.trim());
    } catch (NumberFormatException e) {
      throw new TypeMismatchException(e.getMessage());
    }
  }

  default long getAsLong(String name) throws NoSuchAttributeException, TypeMismatchException {
    if (!getAttributes().containsKey(name)) {
      throw new NoSuchAttributeException(String.format("attribute %s not found", name));
    }
    final String normalized = validateSrc(name);
    try {
      return Long.parseLong(normalized.trim());
    } catch (NumberFormatException e) {
      throw new TypeMismatchException(e.getMessage());
    }
  }

  default BigInteger getAsNumber(String name)
      throws NoSuchAttributeException, TypeMismatchException {
    if (!getAttributes().containsKey(name)) {
      throw new NoSuchAttributeException(String.format("attribute %s not found", name));
    }
    final String normalized = validateSrc(name);
    try {
      return new BigInteger(normalized);
    } catch (NumberFormatException e) {
      throw new TypeMismatchException(e.getMessage());
    }
  }

  default double getAsDouble(String name) throws NoSuchAttributeException, TypeMismatchException {
    if (!getAttributes().containsKey(name)) {
      throw new NoSuchAttributeException(String.format("attribute %s not found", name));
    }
    final String normalized = validateSrc(name);
    try {
      return Double.parseDouble(normalized);
    } catch (NumberFormatException e) {
      throw new TypeMismatchException(e.getMessage());
    }
  }

  default InetAddress getAsInet(String name)
      throws NoSuchAttributeException, TypeMismatchException {
    if (!getAttributes().containsKey(name)) {
      throw new NoSuchAttributeException(String.format("attribute %s not found", name));
    }
    final String normalized = validateSrc(name);
    try {
      if (InetAddresses.isInetAddress(normalized)) {
        return InetAddress.getByName(normalized);
      } else {
        throw new TypeMismatchException("Failed to parse INET value: " + normalized);
      }
    } catch (UnknownHostException ex) {
      throw new TypeMismatchException("Failed to parse INET value: " + normalized);
    }
  }

  default Date getAsDate(String name) throws NoSuchAttributeException, TypeMismatchException {
    if (!getAttributes().containsKey(name)) {
      throw new NoSuchAttributeException(String.format("attribute %s not found", name));
    }
    final String normalized = validateSrc(name);
    try {
      return new Date(LocalDate.parse(normalized).toEpochDay());
    } catch (DateTimeParseException e) {
      // try long
      try {
        return new Date(Long.parseLong(normalized));
      } catch (NumberFormatException | DateTimeException ee) {
        throw new TypeMismatchException(ee.getMessage());
      }
    }
  }

  default UUID getAsUuid(String name) throws NoSuchAttributeException, TypeMismatchException {
    if (!getAttributes().containsKey(name)) {
      throw new NoSuchAttributeException(String.format("attribute %s not found", name));
    }
    final String normalized = validateSrc(name);
    if (normalized.length() != 36) {
      throw new TypeMismatchException("UUID string length must be 36");
    }
    try {
      return UUID.fromString(normalized);
    } catch (IllegalArgumentException e) {
      throw new TypeMismatchException(e.getMessage());
    }
  }

  default boolean getAsBoolean(String name) throws NoSuchAttributeException, TypeMismatchException {
    if (!getAttributes().containsKey(name)) {
      throw new NoSuchAttributeException(String.format("attribute %s not found", name));
    }
    final String normalized = validateSrc(name);
    if (normalized.equalsIgnoreCase("true")) {
      return Boolean.TRUE;
    } else if (normalized.equalsIgnoreCase("false")) {
      return Boolean.FALSE;
    } else {
      throw new TypeMismatchException(
          String.format("For value '%s': %s value must be 'true' or 'false'", normalized, name));
    }
  }

  default ByteBuffer getAsBlob(String name) throws NoSuchAttributeException, TypeMismatchException {
    if (!getAttributes().containsKey(name)) {
      throw new NoSuchAttributeException(String.format("attribute %s not found", name));
    }
    String src = validateSrc(name);
    try {
      return ByteBuffer.wrap(Base64.getDecoder().decode(src.trim()));
    } catch (IllegalArgumentException e) {
      throw new TypeMismatchException(e.getMessage());
    }
  }

  default String validateSrc(String attr) throws TypeMismatchException {
    String out = String.valueOf(getAttributes().get(attr));
    if (out == null || out.isBlank()) {
      throw new TypeMismatchException(attr + " value may not be null or empty");
    }
    return out;
  }
}
