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
package io.isima.bios.data.filter;

import io.isima.bios.models.v1.InternalAttributeType;
import java.math.BigInteger;
import java.util.UUID;
import java.util.function.Predicate;

public class ComparisonFilterTerm implements FilterTerm {

  // We keep these values here for debugging
  final Relation relation;
  final InternalAttributeType type;
  final Object reference;

  private final Predicate<Object> predicate;

  public ComparisonFilterTerm(Relation relation, Object reference, InternalAttributeType type) {
    this.relation = relation;
    this.type = type;
    this.reference = reference;
    this.predicate = compile(relation, reference, type);
  }

  @FunctionalInterface
  private interface Conclusion {
    boolean decide(int comparison);
  }

  private Predicate<Object> compile(
      Relation relation, Object reference, InternalAttributeType type) {
    final Conclusion conclusion;
    switch (relation) {
      case LT:
        conclusion = (comparison) -> comparison < 0;
        break;
      case GT:
        conclusion = (comparison) -> comparison > 0;
        break;
      case LE:
        conclusion = (comparison) -> comparison <= 0;
        break;
      case GE:
        conclusion = (comparison) -> comparison >= 0;
        break;
      default:
        conclusion = (whatever) -> false;
    }

    switch (type) {
      case STRING:
        return (value) -> conclusion.decide(((String) value).compareTo((String) reference));
      case INT:
        return (value) -> conclusion.decide(((Integer) value).compareTo((Integer) reference));
      case LONG:
        return (value) -> conclusion.decide(((Long) value).compareTo((Long) reference));
      case NUMBER:
        return (value) -> conclusion.decide(((BigInteger) value).compareTo((BigInteger) reference));
      case DOUBLE:
        return (value) -> conclusion.decide(((Double) value).compareTo((Double) reference));
      case UUID:
        return (value) -> conclusion.decide(((UUID) value).compareTo((UUID) reference));
      case BOOLEAN:
        return (value) -> conclusion.decide(((Boolean) reference).compareTo((Boolean) reference));
      default:
        throw new UnsupportedOperationException("Type " + type + " unsupported");
    }
  }

  @Override
  public boolean test(Object value) {
    if (value == null) {
      return false;
    }
    return predicate.test(value);
  }

  public enum Relation {
    LT,
    GT,
    LE,
    GE
  }
}
