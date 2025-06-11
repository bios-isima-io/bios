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
package io.isima.bios.models.v1;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.net.InetAddresses;
import io.isima.bios.errors.exception.InvalidValueSyntaxException;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.DataSketchType;
import io.isima.bios.utils.Utils;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import lombok.Getter;

/** Enumeration of attribute types configurable by TFOS administrator. */
public enum InternalAttributeType {
  STRING(true, false, false, AttributeType.STRING, 1) {
    @Override
    public Object parse(String src) throws InvalidValueSyntaxException {
      if (src == null) {
        throw new InvalidValueSyntaxException(biosName() + " value may not be null");
      }
      return src;
    }

    @Override
    public boolean isConvertibleFrom(InternalAttributeType type) {
      return type != BLOB;
    }
  },
  BOOLEAN(true, false, false, AttributeType.BOOLEAN, 2) {
    @Override
    public Boolean parse(final String src) throws InvalidValueSyntaxException {
      final String normalized = validateSrc(src);
      if (normalized.equalsIgnoreCase("true")) {
        return Boolean.TRUE;
      } else if (normalized.equalsIgnoreCase("false")) {
        return Boolean.FALSE;
      } else {
        throw new InvalidValueSyntaxException(
            String.format(
                "For value '%s': %s value must be 'true' or 'false'", normalized, biosName()));
      }
    }

    @Override
    public boolean isConvertibleFrom(InternalAttributeType type) {
      return type == BOOLEAN;
    }
  },
  INT(true, true, false, AttributeType.UNSUPPORTED, 3) {
    @Override
    public Integer parse(final String src) throws InvalidValueSyntaxException {
      final String normalized = validateSrc(src);
      try {
        return Integer.parseInt(normalized.trim());
      } catch (NumberFormatException e) {
        throw new InvalidValueSyntaxException("Input must be a numeric string");
      }
    }

    @Override
    public Integer add(Object left, Object right) {
      Integer l = (left == null) ? Integer.valueOf(0) : (Integer) left;
      return l + (Integer) right;
    }

    @Override
    public boolean isConvertibleFrom(InternalAttributeType type) {
      return type == BOOLEAN || type == INT || type == DATE;
    }
  },
  LONG(true, true, false, AttributeType.INTEGER, 4) {
    @Override
    public Long parse(final String src) throws InvalidValueSyntaxException {
      final String normalized = validateSrc(src);
      try {
        return Long.parseLong(normalized.trim());
      } catch (NumberFormatException e) {
        throw new InvalidValueSyntaxException("Input must be a numeric string");
      }
    }

    @Override
    public String biosName() {
      return "Integer";
    }

    @Override
    public Long add(Object left, Object right) {
      Long l = (left == null) ? Long.valueOf(0) : (Long) left;
      return l + (Long) right;
    }

    @Override
    public boolean isConvertibleFrom(InternalAttributeType type) {
      switch (type) {
        case BOOLEAN:
        case INT:
        case LONG:
        case DATE:
        case TIMESTAMP:
          return true;
        default:
          return false;
      }
    }

    @Override
    public boolean isExtractConversionRequired(Object value) {
      return (value instanceof Integer);
    }

    @Override
    public Object toExtractData(Object value) {
      if (value instanceof Integer) {
        return Long.valueOf((Integer) value);
      } else {
        return value;
      }
    }
  },
  NUMBER(true, true, false, AttributeType.UNSUPPORTED, 5) {
    @Override
    public BigInteger parse(final String src) throws InvalidValueSyntaxException {
      final String normalized = validateSrc(src);
      try {
        return new BigInteger(normalized);
      } catch (NumberFormatException e) {
        throw new InvalidValueSyntaxException(e.getMessage());
      }
    }

    @Override
    public BigInteger add(Object left, Object right) {
      BigInteger l = (left == null) ? BigInteger.ZERO : (BigInteger) left;
      return l.add((BigInteger) right);
    }

    @Override
    public boolean isConvertibleFrom(InternalAttributeType type) {
      switch (type) {
        case BOOLEAN:
        case INT:
        case LONG:
        case NUMBER:
        case DATE:
        case TIMESTAMP:
        case UUID:
          return true;
        default:
          return false;
      }
    }

    @Override
    public boolean isExtractConversionRequired(Object value) {
      return (value instanceof Integer || value instanceof Long);
    }

    @Override
    public Object toExtractData(Object value) {
      if (value instanceof Integer) {
        return BigInteger.valueOf((Integer) value);
      } else if (value instanceof Long) {
        return BigInteger.valueOf((Long) value);
      } else {
        return value;
      }
    }
  },
  DOUBLE(true, true, false, io.isima.bios.models.AttributeType.DECIMAL, 6) {
    @Override
    public Double parse(final String src) throws InvalidValueSyntaxException {
      final String normalized = validateSrc(src);
      try {
        final var value = Double.valueOf(normalized);
        if (value.isNaN() || value.isInfinite()) {
          throw new InvalidValueSyntaxException(String.format("%s value is not allowed", src));
        }
        return value;
      } catch (NumberFormatException e) {
        throw new InvalidValueSyntaxException("Input must be a numeric string");
      }
    }

    @Override
    public String biosName() {
      return "Decimal";
    }

    @Override
    public Double add(Object left, Object right) {
      Double l = (left == null) ? Double.valueOf(0) : (Double) left;
      return l + (Double) right;
    }

    @Override
    public boolean isConvertibleFrom(InternalAttributeType type) {
      switch (type) {
        case INT:
        case LONG:
        case NUMBER:
        case DOUBLE:
          return true;
        default:
          return false;
      }
    }

    @Override
    public boolean isExtractConversionRequired(Object value) {
      return (value instanceof Float);
    }

    @Override
    public Object toExtractData(Object value) {
      if (value instanceof Float) {
        return Double.valueOf((Float) value);
      } else {
        return value;
      }
    }
  },
  INET(true, false, false, io.isima.bios.models.AttributeType.UNSUPPORTED, 7) {
    @Override
    public InetAddress parse(final String src) throws InvalidValueSyntaxException {
      final String normalized = validateSrc(src);
      try {
        if (InetAddresses.isInetAddress(normalized)) {
          return InetAddress.getByName(normalized);
        } else {
          throw new InvalidValueSyntaxException("Failed to parse INET value: " + normalized);
        }
      } catch (UnknownHostException ex) {
        throw new InvalidValueSyntaxException("Failed to parse INET value: " + normalized);
      }
    }

    @Override
    public String toString(final Object data) {
      if (data instanceof InetAddress) {
        return super.toString(InetAddresses.toAddrString((InetAddress) data));
      }
      return super.toString(data);
    }

    @Override
    public boolean isConvertibleFrom(InternalAttributeType type) {
      return type == INT || type == INET;
    }

    @Override
    public boolean isExtractConversionRequired(Object value) {
      return (value instanceof String);
    }

    @Override
    public Object toExtractData(Object value) {
      if (value instanceof String) {
        return InetAddresses.forString((String) value);
      } else {
        return value;
      }
    }
  },
  DATE(true, false, false, io.isima.bios.models.AttributeType.UNSUPPORTED, 8) {
    @Override
    public LocalDate parse(final String src) throws InvalidValueSyntaxException {
      final String normalized = validateSrc(src);
      try {
        return LocalDate.parse(normalized);
      } catch (DateTimeParseException e) {
        // try long
        try {
          return LocalDate.ofEpochDay(Long.parseLong(normalized));
        } catch (NumberFormatException | DateTimeException ee) {
          throw new InvalidValueSyntaxException(ee.getMessage());
        }
      }
    }

    @Override
    public boolean isConvertibleFrom(InternalAttributeType type) {
      return type == INT || type == LONG || type == DATE;
    }

    @Override
    public boolean isExtractConversionRequired(Object value) {
      return (value instanceof String);
    }

    @Override
    public Object toExtractData(Object value) {
      return toExtractDataUsingStringParser(value);
    }
  },
  TIMESTAMP(true, false, false, io.isima.bios.models.AttributeType.UNSUPPORTED, 9) {
    @Override
    public Date parse(final String src) throws InvalidValueSyntaxException {
      final String normalized = validateSrc(src);
      try {
        return new Date(Long.parseLong(normalized));
      } catch (NumberFormatException e) {
        // try ISO date
        try {
          return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX").parse(normalized);
        } catch (ParseException | NumberFormatException ex) {
          throw new InvalidValueSyntaxException(ex.getMessage());
        }
      }
    }

    @Override
    public String toString(final Object data) {
      if (data instanceof Date) {
        return super.toString(((Date) data).getTime());
      } else {
        return super.toString(data);
      }
    }

    @Override
    public boolean isConvertibleFrom(InternalAttributeType type) {
      switch (type) {
        case INT:
        case LONG:
        case DATE:
        case TIMESTAMP:
          return true;
        default:
          return false;
      }
    }

    @Override
    public boolean isExtractConversionRequired(Object value) {
      return (value instanceof Integer || value instanceof String || value instanceof Long);
    }

    @Override
    public Object toExtractData(Object value) {
      if (value instanceof Long) {
        return new Date((Long) value);
      } else if (value instanceof Integer) {
        return new Date((Integer) value);
      } else if (value instanceof String) {
        try {
          return parse((String) value);
        } catch (InvalidValueSyntaxException e) {
          // shouldn't happen
          throw new RuntimeException(e);
        }
      } else {
        return value;
      }
    }
  },
  UUID(true, false, false, io.isima.bios.models.AttributeType.UNSUPPORTED, 10) {
    @Override
    public UUID parse(final String src) throws InvalidValueSyntaxException {
      final String normalized = validateSrc(src);
      if (normalized.length() != 36) {
        throw new InvalidValueSyntaxException("UUID string length must be 36");
      }
      try {
        return java.util.UUID.fromString(normalized);
      } catch (IllegalArgumentException e) {
        throw new InvalidValueSyntaxException(e.getMessage());
      }
    }

    @Override
    public boolean isConvertibleFrom(InternalAttributeType type) {
      return type == UUID;
    }

    @Override
    public boolean isExtractConversionRequired(Object value) {
      return (value instanceof String);
    }

    @Override
    public Object toExtractData(Object value) {
      return toExtractDataUsingStringParser(value);
    }
  },
  ENUM(true, false, true, io.isima.bios.models.AttributeType.STRING, 11) {
    @Override
    public String parse(String src) throws InvalidValueSyntaxException {
      if (src == null) {
        throw new InvalidValueSyntaxException(biosName() + " value may not be null");
      }
      return src;
    }

    @Override
    public boolean isConvertibleFrom(InternalAttributeType type) {
      return type == ENUM;
    }
  },
  BLOB(true, false, false, io.isima.bios.models.AttributeType.BLOB, 12) {
    @Override
    public ByteBuffer parse(String src) throws InvalidValueSyntaxException {
      if (src == null) {
        throw new InvalidValueSyntaxException(biosName() + " value may not be null");
      }
      try {
        return ByteBuffer.wrap(Base64.getDecoder().decode(src.trim()));
      } catch (IllegalArgumentException e) {
        throw new InvalidValueSyntaxException(e.getMessage());
      }
    }

    @Override
    public String toString(final Object data) {
      final Object out;
      if (data instanceof ByteBuffer) {
        final ByteBuffer buf = (ByteBuffer) data;
        final int originalPosition = buf.position();
        final StringBuilder sb = new StringBuilder("0x");
        for (int i = 0; i < TOSTRING_LIMIT / 2 && buf.remaining() > 0; ++i) {
          Utils.byteToHexString(buf.get(), sb);
        }
        if (buf.remaining() > 0) {
          sb.append("...");
        }
        out = sb;
        buf.position(originalPosition);
      } else {
        out = data;
      }
      return super.toString(out);
    }

    @Override
    public boolean isConvertibleFrom(InternalAttributeType type) {
      switch (type) {
        case STRING:
        case INT:
        case LONG:
        case NUMBER:
        case DOUBLE:
        case UUID:
        case BLOB:
          return true;
        default:
          return false;
      }
    }

    @Override
    public boolean isExtractConversionRequired(Object value) {
      return (value instanceof String);
    }

    @Override
    public Object toExtractData(Object value) {
      return toExtractDataUsingStringParser(value);
    }
  },
  ;

  private static final int TOSTRING_LIMIT = 48;

  /** Indicates whether the data type is comparable. */
  private final boolean comparable;

  /** Indicates whether the data type is addable. */
  private final boolean addable;

  /** Indicates whether the data type needs conversion between data plane and data engine. */
  private final boolean conversionNeeded;

  @Getter private final io.isima.bios.models.AttributeType biosAttributeType;

  @Getter private final byte proxy;

  private InternalAttributeType(
      boolean comparable,
      boolean addable,
      boolean conversionNeeded,
      io.isima.bios.models.AttributeType biosAttributeType,
      int proxy) {
    this.comparable = comparable;
    this.addable = addable;
    this.conversionNeeded = conversionNeeded;
    this.biosAttributeType = biosAttributeType;
    this.proxy = (byte) proxy;
  }

  /**
   * Returns whether if the data type of this attribute supports value comparison.
   *
   * @return True if the type supports comparison, false otherwise.
   */
  public boolean isComparable() {
    return comparable;
  }

  /**
   * Returns whether if the data type of this attributes supports value addition.
   *
   * @return True if the type supports addition, false otherwise.
   */
  public boolean isAddable() {
    return addable;
  }

  /**
   * Method that answers if data of the type is convertible from specified type.
   *
   * @param from The type to be converted from
   * @return True if the type is convertible from specified type, false otherwise.
   */
  public abstract boolean isConvertibleFrom(InternalAttributeType from);

  /**
   * Returns whether the data type needs conversion between Data Plane and Data Engine.
   *
   * @return true if the data type needs conversion.
   */
  public boolean needsPlane2EngineConversion() {
    return conversionNeeded;
  }

  private static final Map<Byte, InternalAttributeType> proxyToAttributeTypeMap =
      createProxyToAttributeTypeMap();

  private static Map<Byte, InternalAttributeType> createProxyToAttributeTypeMap() {
    final var map = new HashMap<Byte, InternalAttributeType>();

    for (InternalAttributeType type : values()) {
      map.put(type.proxy, type);
    }

    return map;
  }

  public static InternalAttributeType fromProxy(byte proxy) {
    final var type = proxyToAttributeTypeMap.get(proxy);
    if (type == null) {
      throw new IllegalArgumentException("Proxy " + proxy + " does not exist");
    }
    return type;
  }

  /**
   * Method to convert a string to InternalAttributeType entry.
   *
   * <p>This method is meant be used by JSON deserializer. Unlike simple valueOf() enum method, this
   * method converts input string to an entry in case insensitive manner.
   *
   * @param value Input string
   * @return An {@link InternalAttributeType} entry.
   * @throws IllegalArgumentException when input string does not match any InternalAttributeType
   *     names.
   */
  @JsonCreator
  public static InternalAttributeType forValue(final String value) {
    try {
      return InternalAttributeType.valueOf(value.toUpperCase());
    } catch (IllegalArgumentException ex) {
      throw new IllegalArgumentException(
          String.format("Type %s is unsupported", value.toLowerCase()));
    }
  }

  @JsonValue
  public String toValue() {
    return name().toLowerCase();
  }

  /**
   * Method to convert input string to object of entry's type.
   *
   * <p>This method has package scope since it's meant to be used only by {@link
   * Attributes#convertValue}.
   *
   * @param src Source string
   * @return Converted object.
   * @throws InvalidValueSyntaxException When conversion fails due to invalid source string.
   */
  public abstract Object parse(String src) throws InvalidValueSyntaxException;

  /**
   * Returns type name in biOS API.
   *
   * @return Type name in biOS service.
   */
  String biosName() {
    return name().substring(0, 1) + name().substring(1).toLowerCase();
  }

  /**
   * Method to convert an attribute value to string for logging/display purpose.
   *
   * <p>The method trims output string if the content exceeds 48 characters.
   *
   * @param data input data
   * @return display string
   */
  public String toString(Object data) {
    if (data == null) {
      return "<null>";
    }
    final String out = data.toString();
    if (out.length() > TOSTRING_LIMIT) {
      return out.substring(0, TOSTRING_LIMIT) + "...";
    }
    return out;
  }

  /**
   * Adder used for view calculation.
   *
   * @param left Left value. If null is given, zero value of the entry's internal type is used.
   * @param right Right value. If null is given, NullPointerException is thrown.
   * @return The answer
   * @throws UnsupportedOperationException If the internal type does not support arithmetic
   *     calculation, such as boolean.
   * @throws NullPointerException When parameter right is null.
   */
  public Object add(Object left, Object right) {
    throw new UnsupportedOperationException(name() + " values cannot be added");
  }

  /**
   * aaGeneric method to check validity of input string.
   *
   * @param src Source string
   * @return Trimmed source string
   * @throws InvalidValueSyntaxException If the input string is null or trimmed string is empty
   */
  protected String validateSrc(String src) throws InvalidValueSyntaxException {
    String out;
    if (src == null || (out = src.trim()).isEmpty()) {
      throw new InvalidValueSyntaxException(biosName() + " value may not be null or empty");
    }
    return out;
  }

  /**
   * Returns false, if the object is already in expected form by the client.
   *
   * @param value value obtained from the server
   * @return false, if it is already in expected form, true otherwise
   */
  public boolean isExtractConversionRequired(Object value) {
    return false;
  }

  public Object toExtractData(Object value) {
    return value;
  }

  protected Object toExtractDataUsingStringParser(Object value) {
    if (value instanceof String) {
      try {
        return parse((String) value);
      } catch (InvalidValueSyntaxException e) {
        // shouldn't happen
        throw new RuntimeException(e);
      }
    } else {
      return value;
    }
  }

  public boolean isSupportedBySketch(DataSketchType sketchType) {
    switch (sketchType) {
      case MOMENTS:
      case QUANTILES:
        if ((this == LONG) || (this == DOUBLE)) {
          return true;
        }
        return false;
      case DISTINCT_COUNT:
      case SAMPLE_COUNTS:
        if ((this == LONG) || (this == DOUBLE) || (this == STRING) || (this == ENUM)) {
          return true;
        }
        return false;
      case LAST_N:
        return this != BLOB;
      case NONE:
        return false;
      default:
        throw new UnsupportedOperationException();
    }
  }
}
