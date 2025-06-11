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
package io.isima.bios.sdk;

import java.util.List;

public class Bios {
  private static final ISql STATEMENT_STARTER = new ISql();

  /**
   * Returns a builder of a new biOS session.
   *
   * @param host Host name of the endpoint, e.g. "bios.isima.io"
   * @return A new session builder
   */
  public static Session.Starter newSession(String host) {
    return new Session.Starter(host);
  }

  /**
   * Start writing an isql statement.
   *
   * @return The statement starter (a builder object).
   */
  public static ISql isql() {
    return STATEMENT_STARTER;
  }

  /**
   * Specifies keys in the where clause for Context selects. The object returned is not meant to be
   * kept around and re-used; it is meant to be used as a helper within the where clause function
   * call as a parameter declared in-place.
   */
  public static ISqlHelperDoNotDirectlyUseThisClassOrSubclassesInApplication
          .ContextWhereClauseBuilder
      keys() {
    return new ISqlHelperDoNotDirectlyUseThisClassOrSubclassesInApplication
        .ContextWhereClauseBuilder();
  }

  public static CompositeKey key(Object... elements) {
    return CompositeKey.of(elements);
  }

  public static CompositeKey key(List<Object> elements) {
    return CompositeKey.of(elements);
  }

  public static Statement rollbackOnFailure(Statement statement) {
    return new ISql.AtomicMutation(statement);
  }

  /*
  public static SdkCountersBuilder newCounterService() {
    return new SdkCountersBuilder();
  }
  */
}
