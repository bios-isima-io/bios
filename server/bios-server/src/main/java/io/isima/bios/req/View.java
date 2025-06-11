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
package io.isima.bios.req;

import io.isima.bios.models.ViewFunction;
import java.util.List;

public interface View {
  ViewFunction getFunction();

  String getBy();

  List<String> getDimensions();

  Boolean getReverse();

  Boolean getCaseSensitive();

  interface Op<B, Y> {
    Y sortBy(String attribute);

    B distinctBy(List<String> dimensions);

    B distinctBy(String... dimensions);

    B groupBy(List<String> dimensions);

    B groupBy(String... dimensions);
  }

  interface SortBuild {
    SortBuild reverse(boolean reverse);

    SortBuild caseSensitive(boolean caseSensitive);

    View build();
  }

  interface Build {
    View build();
  }

  interface SortEnd<P> {
    SortEnd<P> reverse(boolean reverse);

    SortEnd<P> caseSensitive(boolean caseSensitive);

    P end();
  }

  interface End<P> {
    P end();
  }

  interface Builder extends Op<Build, SortBuild>, SortBuild, Build {}

  // Builder that is nested with the parent for nested fluency
  interface LinkedBuilder<P> extends Op<End<P>, SortEnd<P>>, SortEnd<P>, End<P> {}

  static Op<Build, SortBuild> op() {
    return BuilderProvider.getBuilderProvider().getViewBuilder();
  }
}
