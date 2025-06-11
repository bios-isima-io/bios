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
package io.isima.bios.query;

import io.isima.bios.models.MetricFunction;
import io.isima.bios.models.v1.Aggregate;
import java.util.regex.Pattern;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

@Getter
@AllArgsConstructor
public class CompiledSortRequest {
  private static final Pattern PATTERN =
      Pattern.compile("^\\s*([0-9a-zA-Z_\\.:]+)\\s*(?:\\(\\s*([0-9a-zA-Z_\\.:]*)\\s*\\))?\\s*$");

  /**
   * Compile a sort specification.
   *
   * @param by The sort key
   * @param isReverse Specifies to sort in reverse order
   * @param isCaseSensitive Specifies to consider cases in string sorting
   * @return Compiled sort request
   * @throws CompilationException thrown to indicate that parsing the specification failed
   */
  public static CompiledSortRequest compile(String by, boolean isReverse, boolean isCaseSensitive)
      throws CompilationException {
    if (StringUtils.isBlank(by)) {
      throw new CompilationException("Order key is not set");
    }
    final var match = PATTERN.matcher(by);
    if (!match.find()) {
      throw new CompilationException("Syntax error");
    }
    final var functionOrAttribute = match.group(1);
    final var domain = match.group(2);
    final Aggregate aggregate;
    if (domain == null) {
      // plain attribute
      aggregate = new Aggregate(null, functionOrAttribute);
    } else {
      // aggregate function
      try {
        final var function = MetricFunction.valueOf(functionOrAttribute.toUpperCase());
        if (function != MetricFunction.COUNT && StringUtils.isBlank(domain)) {
          throw new CompilationException(
              String.format("Function '%s' requires a parameter", function.name().toLowerCase()));
        }
        aggregate = new Aggregate(function, domain);
      } catch (IllegalArgumentException e) {
        throw new CompilationException("Unknown function: " + functionOrAttribute);
      }
    }
    aggregate.setAs(by);
    return new CompiledSortRequest(aggregate, isReverse, isCaseSensitive);
  }

  private Aggregate aggregate;
  private boolean isReverse;
  private boolean isCaseSensitive;

  public boolean isKeyAttribute() {
    return aggregate.getFunction() == null;
  }

  public boolean isKeyAggregate() {
    return aggregate.getFunction() != null;
  }

  public static class CompilationException extends Exception {
    public CompilationException(String message) {
      super(message);
    }
  }
}
