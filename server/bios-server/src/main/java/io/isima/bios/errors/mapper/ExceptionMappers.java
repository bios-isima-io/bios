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
package io.isima.bios.errors.mapper;

import java.util.Set;

public class ExceptionMappers {

  public static void addExceptionMappers(Set<Object> providerSet) {
    providerSet.add(new BiAppExceptionMapper());
    providerSet.add(new JsonMappingExceptionMapper());
    providerSet.add(new JsonParseExceptionMapper());
    providerSet.add(new NotAuthorizedExceptionMapper());
    providerSet.add(new ValidationExceptionMapper());
  }
}
