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
package io.isima.bios.data.impl.storage;

import io.isima.bios.data.filter.ComparisonFilterTerm;
import io.isima.bios.data.filter.EqualsFilterTerm;
import io.isima.bios.data.filter.FilterTerm;
import io.isima.bios.errors.exception.InvalidEnumException;
import io.isima.bios.errors.exception.InvalidFilterException;
import io.isima.bios.errors.exception.InvalidValueSyntaxException;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.Attributes;
import java.util.HashSet;
import org.apache.cassandra.cql3.Relation;
import org.apache.cassandra.cql3.Term;

public class CqlFilterCompiler {

  public static FilterTerm createTerm(Relation relation, AttributeDesc attribute)
      throws InvalidFilterException {

    switch (relation.operator()) {
      case EQ:
        {
          return new EqualsFilterTerm(getValue(relation.getValue(), attribute));
        }
      case IN:
        {
          final var values = new HashSet<Object>();
          for (Term.Raw elem : relation.getInValues()) {
            values.add(getValue(elem, attribute));
          }
          return new EqualsFilterTerm(values);
        }
      case LT:
        return new ComparisonFilterTerm(
            ComparisonFilterTerm.Relation.LT,
            getValue(relation.getValue(), attribute),
            attribute.getAttributeType());
      case LTE:
        return new ComparisonFilterTerm(
            ComparisonFilterTerm.Relation.LE,
            getValue(relation.getValue(), attribute),
            attribute.getAttributeType());
      case GT:
        return new ComparisonFilterTerm(
            ComparisonFilterTerm.Relation.GT,
            getValue(relation.getValue(), attribute),
            attribute.getAttributeType());
      case GTE:
        return new ComparisonFilterTerm(
            ComparisonFilterTerm.Relation.GE,
            getValue(relation.getValue(), attribute),
            attribute.getAttributeType());
      default:
        throw new InvalidFilterException("Operator " + relation.operator() + " is not supported");
    }
  }

  public static Object getValue(Term.Raw src, AttributeDesc attribute)
      throws InvalidFilterException {
    String value = src.getText();
    final boolean quoted = value.startsWith("'") && value.endsWith("'");
    final var type = attribute.getAttributeType();
    final boolean isQuotedExpected;
    switch (type) {
      case STRING:
      case ENUM:
      case UUID:
      case INET:
        isQuotedExpected = true;
        break;
      default:
        isQuotedExpected = false;
    }
    if (quoted != isQuotedExpected) {
      String whether = isQuotedExpected ? "" : " not";
      throw new InvalidFilterException(
          String.format(
              "Value of attribute %s must%s be specified with a quoted string",
              attribute.getName(), whether));
    }
    if (quoted) {
      value = value.substring(1, value.length() - 1);
    }
    try {
      return Attributes.convertValue(value, attribute);
    } catch (InvalidValueSyntaxException | InvalidEnumException e) {
      // This shouldn't happen. The filter is already validated when we reach here
      throw new RuntimeException(e);
    }
  }
}
