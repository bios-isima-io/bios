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
package io.isima.bios.codec.proto.isql;

import io.isima.bios.codec.proto.wrappers.ProtoInsertQuery;
import io.isima.bios.models.isql.ISqlStatement;
import io.isima.bios.models.isql.ISqlStatement.InsertBuilder;
import io.isima.bios.models.isql.InsertStatement;
import io.isima.bios.models.isql.InsertStatement.InsertData;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import org.apache.commons.text.StringEscapeUtils;

/** Builds a normal insert query builder. */
public class ProtoInsertQueryBuilder
    implements ISqlStatement.InsertBuilder, InsertStatement.LinkedBuilder {
  private final BasicBuilderValidator validator;

  private String into;
  private String csv;
  private List<String> csvs;

  /**
   * Protobuf based insert query builder as the query parameters are directly filled into the
   * protobuf query message object, where possible.
   *
   * @param validator Validator methods
   */
  ProtoInsertQueryBuilder(BasicBuilderValidator validator) {
    this.validator = validator;
    this.into = null;
    this.csvs = null;
    this.csv = null;
  }

  /**
   * CSV based private constructor.
   *
   * @param other Other constructor
   * @param csv Csv string
   */
  private ProtoInsertQueryBuilder(ProtoInsertQueryBuilder other, String csv) {
    this.into = other.into;
    this.validator = other.validator;
    this.csvs = other.csvs;
    this.csv = csv;
    validator.validateStringParam(csv, "csv");
  }

  /** CSV bulk based private constructor. */
  private ProtoInsertQueryBuilder(ProtoInsertQueryBuilder other, List<String> csvBulk) {
    this.into = other.into;
    this.validator = other.validator;
    this.csvs = Collections.unmodifiableList(csvBulk);
    validator.validateStringList(csvBulk, "csvBulk");
  }

  private ProtoInsertQueryBuilder(ProtoInsertQueryBuilder other, String... csvBulk) {
    this.into = other.into;
    this.validator = other.validator;
    this.csvs = other.csvs;
    this.validator.validateObject(csvBulk, "csvBulk");
    this.csvs = List.of(csvBulk);
    validator.validateStringList(this.csvs, "csvBulk");
  }

  @Override
  public InsertData into(String into) {
    validator.validateStringParam(into, "into");
    this.into = into;
    return this;
  }

  @Override
  public InsertBuilder csv(String text) {
    // construct again to narrow type to InsertResponse
    return new ProtoInsertQueryBuilder(this, text);
  }

  @Override
  public InsertBuilder csvBulk(List<String> texts) {
    return new ProtoInsertQueryBuilder(this, texts);
  }

  @Override
  public InsertBuilder csvBulk(String... texts) {
    return new ProtoInsertQueryBuilder(this, texts);
  }

  @Override
  public InsertBuilder values(final List<Object> values) {
    return csv(csvFromValues(values));
  }

  @Override
  public InsertBuilder values(final Object... values) {
    return values(List.of(values));
  }

  @Override
  public InsertBuilder valuesBulk(final List<List<Object>> valuesList) {
    final List<String> csvList = new ArrayList<>();
    for (final var values : valuesList) {
      csvList.add(csvFromValues(values));
    }
    return csvBulk(csvList);
  }

  @Override
  public InsertStatement build() {
    if (csv != null) {
      return new ProtoInsertQuery(into, csv);
    } else {
      return new ProtoInsertQuery(into, csvs);
    }
  }

  /**
   * Creates an RFC-4180-compliant CSV string for all the values passed in, taking care of escaping.
   */
  public static String csvFromValues(final List<Object> values) {
    StringBuilder sb = new StringBuilder();
    String delimiter = "";
    for (Object o : values) {
      sb.append(delimiter);
      delimiter = ",";
      if (o == null) {
        throw new IllegalArgumentException("Null value not allowed in insert.");
      }
      if ((o instanceof Long) || (o instanceof Double) || (o instanceof Boolean)) {
        sb.append(o.toString());
        continue;
      }
      if (o instanceof ByteBuffer) {
        final byte[] arr = new byte[((ByteBuffer) o).remaining()];
        ((ByteBuffer) o).get(arr);
        sb.append(Base64.getEncoder().encodeToString(arr));
        continue;
      }
      if (o instanceof String) {
        sb.append(StringEscapeUtils.escapeCsv((String) o));
        continue;
      }
      throw new IllegalArgumentException(
          "Value with invalid object type "
              + o.getClass().getName()
              + " received in insert; allowed types are String, Long, Double, Boolean, and ByteBuffer");
    }
    return sb.toString();
  }
}
