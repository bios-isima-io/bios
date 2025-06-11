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
package io.isima.bios.common;

import static io.isima.bios.models.v1.InternalAttributeType.BOOLEAN;
import static io.isima.bios.models.v1.InternalAttributeType.INET;
import static io.isima.bios.models.v1.InternalAttributeType.INT;
import static io.isima.bios.models.v1.InternalAttributeType.UUID;

import com.google.common.net.InetAddresses;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TfosUtils {
  private static final Logger logger = LoggerFactory.getLogger(TfosUtils.class);

  /**
   * Returns the application version string.
   *
   * @return Version string.
   */
  public static String version() {
    final String propertyFile = "version.properties";
    String version = "";
    try (InputStream inputStream =
        TfosUtils.class.getClassLoader().getResourceAsStream(propertyFile)) {
      if (inputStream == null) {
        return version;
      }
      Properties properties = new Properties();
      properties.load(inputStream);
      version = properties.getProperty("version");
    } catch (IOException e) {
      logger.error("Error in reading property file {}", propertyFile, e);
    }
    return version;
  }

  /**
   * Method to convert byte data to a hex string.
   *
   * <p>The output is accumulated into the specified string builder.
   *
   * @param data Input data
   * @param sb StringBuilder to accumulate the output.
   */
  public static void byteToHexString(final byte data, final StringBuilder sb) {
    sb.append(toHex((data & 0xff) >> 4)).append(toHex(data & 0xf));
  }

  private static char toHex(int src) {
    return (src < 10) ? (char) (src + '0') : (char) ((src - 10) + 'a');
  }

  /**
   * Method used to convert an attribute value to another type.
   *
   * @param src Source value
   * @param toDesc Attribute descriptor for the modification target type
   * @return
   */
  public static Object convertAttribute(Object src, AttributeDesc fromDesc, AttributeDesc toDesc) {
    if (toDesc == null) {
      throw new IllegalArgumentException("Parameter toDesc may not be null");
    }
    final InternalAttributeType fromType = fromDesc.getAttributeType();
    final Object fromData;
    if (fromType == InternalAttributeType.ENUM) {
      fromData = fromDesc.getEnum().get((Integer) src);
    } else {
      fromData = src;
    }
    final InternalAttributeType toType = toDesc.getAttributeType();
    switch (toType) {
      case STRING:
        return stringConversion(fromData, fromType, toType);
      case BOOLEAN:
        return fromType == BOOLEAN ? src : unsupportedConversion(fromType, toType);
      case INT:
        return intConversion(fromData, fromType, toType);
      case LONG:
        return longConversion(fromData, fromType, toType);
      case NUMBER:
        return numberConversion(fromData, fromType, toType);
      case DOUBLE:
        return doubleConversion(fromData, fromType, toType);
      case INET:
        return inetConversion(fromData, fromType, toType);
      case DATE:
        return dateConversion(fromData, fromType, toType);
      case TIMESTAMP:
        return timestampConversion(fromData, fromType, toType);
      case UUID:
        return fromType == UUID ? src : unsupportedConversion(fromType, toType);
      case ENUM:
        return fromType == InternalAttributeType.ENUM
            ? fromData
            : unsupportedConversion(fromType, toType);
      case BLOB:
        return blobConversion(fromData, fromType, toType);
      default:
        return unsupportedConversion(fromType, toType);
    }
    // return toDesc.getAttributeType().convertFrom(fromData, fromDesc.getAttributeType());
  }

  private static String stringConversion(
      Object src, InternalAttributeType fromType, InternalAttributeType toType) {
    switch (fromType) {
      case BLOB:
        return unsupportedConversion(fromType, toType);
      case INET:
        return InetAddresses.toAddrString((InetAddress) src);
      case TIMESTAMP:
        return Long.toString(((Date) src).getTime());
      default:
        return src.toString();
    }
  }

  private static Integer intConversion(
      Object src, InternalAttributeType fromType, InternalAttributeType toType) {
    switch (fromType) {
      case BOOLEAN:
        return ((Boolean) src) ? Integer.valueOf(1) : Integer.valueOf(0);
      case INT:
        return (Integer) src;
      case DATE:
        final com.datastax.driver.core.LocalDate date = (com.datastax.driver.core.LocalDate) src;
        return Integer.valueOf(date.getDaysSinceEpoch());
      default:
        return unsupportedConversion(fromType, toType);
    }
  }

  private static Long longConversion(
      Object src, InternalAttributeType fromType, InternalAttributeType toType) {
    switch (fromType) {
      case BOOLEAN:
        return Boolean.TRUE.equals(src) ? Long.valueOf(1L) : Long.valueOf(0L);
      case INT:
        return Long.valueOf(((Integer) src));
      case LONG:
        return (Long) src;
      case DATE:
        final com.datastax.driver.core.LocalDate date = (com.datastax.driver.core.LocalDate) src;
        return (long) date.getDaysSinceEpoch();
      case TIMESTAMP:
        return ((Date) src).getTime();
      default:
        return unsupportedConversion(fromType, toType);
    }
  }

  private static BigInteger numberConversion(
      Object src, InternalAttributeType fromType, InternalAttributeType toType) {
    switch (fromType) {
      case BOOLEAN:
        return BigInteger.valueOf(Boolean.TRUE.equals(src) ? 1L : 0L);
      case INT:
        return BigInteger.valueOf((Integer) src);
      case LONG:
        return BigInteger.valueOf((Long) src);
      case NUMBER:
        return (BigInteger) src;
      case DATE:
        final com.datastax.driver.core.LocalDate date = (com.datastax.driver.core.LocalDate) src;
        return BigInteger.valueOf(date.getDaysSinceEpoch());
      case TIMESTAMP:
        return BigInteger.valueOf(((Date) src).getTime());
      case UUID:
        final UUID uuid = (UUID) src;
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return new BigInteger(bb.array());
      default:
        return unsupportedConversion(fromType, toType);
    }
  }

  private static Double doubleConversion(
      Object src, InternalAttributeType fromType, InternalAttributeType toType) {
    switch (fromType) {
      case INT:
        return Double.valueOf(((Integer) src));
      case LONG:
        return Double.valueOf(((Long) src));
      case NUMBER:
        return Double.valueOf(((BigInteger) src).toString());
      case DOUBLE:
        return (Double) src;
      default:
        return unsupportedConversion(fromType, toType);
    }
  }

  private static InetAddress inetConversion(
      Object src, InternalAttributeType fromType, InternalAttributeType toType) {
    if (fromType == INT) {
      return InetAddresses.fromInteger((Integer) src);
    } else if (fromType == INET) {
      return (InetAddress) src;
    } else {
      return unsupportedConversion(fromType, toType);
    }
  }

  private static com.datastax.driver.core.LocalDate dateConversion(
      Object src, InternalAttributeType fromType, InternalAttributeType toType) {
    switch (fromType) {
      case INT:
        return com.datastax.driver.core.LocalDate.fromDaysSinceEpoch((Integer) src);
      case LONG:
        return com.datastax.driver.core.LocalDate.fromDaysSinceEpoch(((Long) src).intValue());
      case DATE:
        final com.datastax.driver.core.LocalDate date = (com.datastax.driver.core.LocalDate) src;
        return date;
      default:
        return unsupportedConversion(fromType, toType);
    }
  }

  private static Date timestampConversion(
      Object src, InternalAttributeType fromType, InternalAttributeType toType) {
    switch (fromType) {
      case INT:
        return new Date((Integer) src);
      case LONG:
        return new Date((Long) src);
      case DATE:
        final com.datastax.driver.core.LocalDate date = (com.datastax.driver.core.LocalDate) src;
        return new Date(date.getMillisSinceEpoch());
      case TIMESTAMP:
        return (Date) src;
      default:
        return unsupportedConversion(fromType, toType);
    }
  }

  private static ByteBuffer blobConversion(
      Object src, InternalAttributeType fromType, InternalAttributeType toType) {
    switch (fromType) {
      case STRING:
        return ByteBuffer.wrap(((String) src).getBytes());
      case INT:
        return ByteBuffer.allocate(Integer.SIZE / 8).putInt((Integer) src);
      case LONG:
        return ByteBuffer.allocate(Long.SIZE / 8).putLong((Long) src);
      case NUMBER:
        return ByteBuffer.wrap(((BigInteger) src).toByteArray());
      case DOUBLE:
        return ByteBuffer.allocate(Double.SIZE / 8).putDouble((Double) src);
      case UUID:
        final UUID uuid = (UUID) src;
        return ByteBuffer.wrap(new byte[16])
            .putLong(uuid.getMostSignificantBits())
            .putLong(uuid.getLeastSignificantBits());
      case BLOB:
        return (ByteBuffer) src;
      default:
        return unsupportedConversion(fromType, toType);
    }
  }

  private static <T extends Object> T unsupportedConversion(
      InternalAttributeType from, InternalAttributeType to) {
    throw new UnsupportedOperationException(
        String.format("Conversion of data from %s to %s is unsupported", from.name(), to.name()));
  }
}
