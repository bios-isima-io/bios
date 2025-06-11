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
package io.isima.bios.data.impl.models;

import io.isima.bios.admin.v1.StreamDesc;
import java.util.List;
import lombok.AllArgsConstructor;

/**
 * Container that carries context select operation resources.
 *
 * <p>While deciding the select methodology, the process leaves byproducts that can be used later by
 * the concrete select method. These byproducts are passed to avoid calculating twice.
 *
 * <p>This class is also used to indicate which methodology to use. Only one of the resource objects
 * is non-null. The corresponding method is chosen.
 */
@AllArgsConstructor
public class ContextSelectOpResources {
  /** for generic context select */
  public final String statement;

  /** for extracting by primary keys */
  public final List<List<Object>> primaryKey;

  /** for indexed query */
  public final StreamDesc indexStreamDesc;

  /** for feature extraction */
  public final ContextFeatureQueryParams featureQueryParams;

  /** for sketches extraction */
  public final UseSketchPreference sketchPreference;

  /** Constructs an instance for generic select. */
  public static ContextSelectOpResources of(String statement) {
    return new ContextSelectOpResources(statement, null, null, null, null);
  }

  /** Constructs an instance for extracting by primary keys. */
  public static ContextSelectOpResources of(List<List<Object>> primaryKey) {
    return new ContextSelectOpResources(null, primaryKey, null, null, null);
  }

  /** Constructs an instance for indexed select. */
  public static ContextSelectOpResources of(StreamDesc indexStreamDesc) {
    return new ContextSelectOpResources(null, null, indexStreamDesc, null, null);
  }

  /** Constructs an instance for feature extraction. */
  public static ContextSelectOpResources of(ContextFeatureQueryParams params) {
    return new ContextSelectOpResources(null, null, null, params, null);
  }

  /** Constructs an instance for sketches extraction. */
  public static ContextSelectOpResources of(UseSketchPreference preference) {
    return new ContextSelectOpResources(null, null, null, null, preference);
  }
}
