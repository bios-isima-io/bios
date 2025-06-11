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
package io.isima.bios.admin.v1;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * Class to keep parameters used for FeatureAsContext.
 *
 * <p>Parameters are used for several purposes such as
 *
 * <ul>
 *   <li>Referred by context to know the context entry TTL
 *   <li>Referred by a feature to know there's any other feature that is referring the context
 *       already
 * </ul>
 *
 * <p>These parameters in this class are loosely coupled with the context using a map in the
 * AdminInternal module because the parameters are created/modified/deleted by the referring feature
 * after the context is created. The description of the context (StreamDesc) should be treated as an
 * immutable object after being created, then post-created feature-as-context parameters have to be
 * separated here.
 */
@Getter
@Setter
@AllArgsConstructor
public class FeatureAsContextInfo {
  /** Feature name that refers to the context. */
  private String referrer;

  /** Context entry time to live in milliseconds. */
  private Long ttlInMillis;
}
