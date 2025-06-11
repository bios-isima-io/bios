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
package io.isima.bios.service;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class CorsOriginWhiteList {

  private final boolean allowAll;
  private final Set<CharSequence> whiteList;

  public CorsOriginWhiteList(String[] whitelist) {
    final var temp = new HashSet<CharSequence>();
    for (CharSequence entry : whitelist) {
      if ("*".equals(entry)) {
        allowAll = true;
        whiteList = null;
        return;
      }
      if (entry == null) {
        continue;
      }
      temp.add(entry);
    }
    whiteList = Collections.unmodifiableSet(temp);
    allowAll = false;
  }

  public boolean isAllowed(CharSequence origin) {
    return allowAll || (origin != null && whiteList.contains(origin));
  }
}
