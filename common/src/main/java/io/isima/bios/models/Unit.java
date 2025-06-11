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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.isima.bios.utils.EnumStringifier;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/** The real-world kind of values, currently only applicable to quantity category. */
@Getter
@RequiredArgsConstructor
public enum Unit {
  OTHER_UNIT("", AttributeKind.OTHER_KIND),

  U_S_D("$", AttributeKind.MONEY),
  E_U_R("€", AttributeKind.MONEY),
  I_N_R("₹", AttributeKind.MONEY),
  J_P_Y("¥", AttributeKind.MONEY),

  UNIX_SECOND("unixSec", AttributeKind.TIMESTAMP),
  UNIX_MILLISECOND("unixMs", AttributeKind.TIMESTAMP),
  UUID_TIME("uuidTime", AttributeKind.TIMESTAMP),

  NANOSECOND("ns", AttributeKind.DURATION),
  MICROSECOND("µs", AttributeKind.DURATION),
  MILLISECOND("ms", AttributeKind.DURATION),
  SECOND("s", AttributeKind.DURATION),
  MINUTE("min", AttributeKind.DURATION),
  HOUR("hr", AttributeKind.DURATION),
  DAY("day", AttributeKind.DURATION),
  WEEK("wk", AttributeKind.DURATION),
  MONTH("month", AttributeKind.DURATION),
  QUARTER("quarter", AttributeKind.DURATION),
  YEAR("yr", AttributeKind.DURATION),

  HZ("Hz", AttributeKind.FREQUENCY),
  PER_SECOND("/s", AttributeKind.FREQUENCY),
  PER_MINUTE("/min", AttributeKind.FREQUENCY),
  PER_HOUR("/hr", AttributeKind.FREQUENCY),
  PER_DAY("/day", AttributeKind.FREQUENCY),
  PER_WEEK("/wk", AttributeKind.FREQUENCY),
  PER_MONTH("/month", AttributeKind.FREQUENCY),
  PER_QUARTER("/quarter", AttributeKind.FREQUENCY),
  PER_YEAR("/yr", AttributeKind.FREQUENCY),

  COUNT("count", AttributeKind.DIMENSIONLESS),
  RANK("rank", AttributeKind.DIMENSIONLESS),
  PERCENT("%", AttributeKind.DIMENSIONLESS),
  RATIO("", AttributeKind.DIMENSIONLESS),
  PLAIN_NUMBER("", AttributeKind.DIMENSIONLESS),

  LATITUDE_DEGREE("°", AttributeKind.LATITUDE),

  LONGITUDE_DEGREE("°", AttributeKind.LONGITUDE),

  NANOMETRE("nm", AttributeKind.DISTANCE),
  MICROMETRE("µm", AttributeKind.DISTANCE),
  MILLIMETRE("mm", AttributeKind.DISTANCE),
  CENTIMETRE("cm", AttributeKind.DISTANCE),
  METRE("m", AttributeKind.DISTANCE),
  KILOMETRE("km", AttributeKind.DISTANCE),

  INCH("in", AttributeKind.DISTANCE),
  FOOT("ft", AttributeKind.DISTANCE),
  YARD("yd", AttributeKind.DISTANCE),
  FURLONG("furlong", AttributeKind.DISTANCE),
  MILE("mi", AttributeKind.DISTANCE),
  NAUTICAL_MILE("nmi", AttributeKind.DISTANCE),
  LEAGUE("league", AttributeKind.DISTANCE),
  NAUTICAL_LEAGUE("nleague", AttributeKind.DISTANCE),

  METRES_PER_SECOND("m/s", AttributeKind.SPEED),
  KILOMETRES_PER_HOUR("kmph", AttributeKind.SPEED),
  MILES_PER_HOUR("mph", AttributeKind.SPEED),
  KNOTS("mn", AttributeKind.SPEED),
  FEET_PER_SECOND("ft/s", AttributeKind.SPEED),

  MILLILITRE("ml", AttributeKind.VOLUME),
  LITRE("l", AttributeKind.VOLUME),
  CUBIC_METRE("m^3", AttributeKind.VOLUME),

  FLUID_OUNCE("fl oz", AttributeKind.VOLUME),
  PINT("pt", AttributeKind.VOLUME),
  QUART("qt", AttributeKind.VOLUME),
  GALLON("gal", AttributeKind.VOLUME),
  CUBIC_INCH("in^3", AttributeKind.VOLUME),
  CUBIC_FOOT("ft^3", AttributeKind.VOLUME),
  CUBIC_YARD("yd^3", AttributeKind.VOLUME),

  MICROGRAM("µg", AttributeKind.WEIGHT),
  MILLIGRAM("mg", AttributeKind.WEIGHT),
  GRAM("g", AttributeKind.WEIGHT),
  KILOGRAM("kg", AttributeKind.WEIGHT),
  QUINTAL("q", AttributeKind.WEIGHT),
  METRIC_TONNE("tonne", AttributeKind.WEIGHT),

  OUNCE("oz", AttributeKind.WEIGHT),
  POUND("lb", AttributeKind.WEIGHT),
  TON("t", AttributeKind.WEIGHT),
  TROY_OUNCE("ozt", AttributeKind.WEIGHT),

  BYTE("byte", AttributeKind.DATA_SIZE),
  KILOBYTE("kB", AttributeKind.DATA_SIZE),
  MEGABYTE("mB", AttributeKind.DATA_SIZE),
  GIGABYTE("gB", AttributeKind.DATA_SIZE),
  TERABYTE("tB", AttributeKind.DATA_SIZE),
  PETABYTE("pB", AttributeKind.DATA_SIZE),

  KIBIBYTE("KiB", AttributeKind.DATA_SIZE),
  MEBIBYTE("MiB", AttributeKind.DATA_SIZE),
  GIBIBYTE("GiB", AttributeKind.DATA_SIZE),
  TEBIBYTE("TiB", AttributeKind.DATA_SIZE),
  PEBIBYTE("PiB", AttributeKind.DATA_SIZE),

  BITS_PER_SECOND("bit/s", AttributeKind.DATA_RATE),
  KILOBITS_PER_SECOND("kbit/s", AttributeKind.DATA_RATE),
  MEGABITS_PER_SECOND("Mbit/s", AttributeKind.DATA_RATE),
  GIGABITS_PER_SECOND("Gbit/s", AttributeKind.DATA_RATE),
  TERABITS_PER_SECOND("Tbit/s", AttributeKind.DATA_RATE),

  PIXEL("px", AttributeKind.DISPLAY_SIZE);

  private final String displayName;
  private final AttributeKind kind;

  public static String getDisplayName(Unit unit) {
    if (unit == null) {
      return "";
    }
    return unit.getDisplayName();
  }

  public static AttributeSummary getFirstSummary(AttributeCategory category, Unit unit) {
    if ((category == AttributeCategory.DIMENSION) || (category == AttributeCategory.DESCRIPTION)) {
      return AttributeSummary.WORD_CLOUD;
    }

    assert (category == AttributeCategory.QUANTITY);
    if (unit == null) {
      return AttributeSummary.AVG;
    }

    final var kind = unit.getKind();
    switch (kind) {
      case DURATION:
      case FREQUENCY:
      case LATITUDE:
      case LONGITUDE:
      case SPEED:
      case DATA_SIZE:
      case DATA_RATE:
      case DISPLAY_SIZE:
      default:
        return AttributeSummary.AVG;

      case MONEY:
      case DISTANCE:
      case VOLUME:
      case WEIGHT:
        return AttributeSummary.SUM;

      case TIMESTAMP:
        return AttributeSummary.TIMESTAMP_LAG;

      case DIMENSIONLESS:
        switch (unit) {
          case RANK:
          case PERCENT:
          case RATIO:
          case PLAIN_NUMBER:
          default:
            return AttributeSummary.AVG;
          case COUNT:
            return AttributeSummary.SUM;
        }
    }
  }

  public static AttributeSummary getSecondSummary(AttributeCategory category, Unit unit) {
    return null;
  }

  private static final EnumStringifier<Unit> stringifier = new EnumStringifier<>(values());

  @JsonCreator
  public static Unit forValue(String value) {
    return stringifier.destringify(value);
  }

  @JsonValue
  public String stringify() {
    return stringifier.stringify(this);
  }
}
