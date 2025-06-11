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
package io.isima.bios.data.synthesis.generators;

import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.models.BiosStreamConfig;
import java.util.Map;

/**
 * Interface used for injecting an external AttributeGenerator resolver to the Generator Resolver.
 */
public interface GeneratorResolver {

  /**
   * Externally resolves attribute generators.
   *
   * <p>The method may skip generators for some attributes. The master resolver would fill the
   * missing generators with the default generators. But the method must not return null.
   *
   * @param tenantName Tenant name
   * @param streamConfig Configuration of the target BIOS stream.
   * @return Map of attribute name and corresponding attribute generator.
   * @throws TfosException thrown to indicate that some user error happened
   * @throws ApplicationException thrown to indicate that some unexpected error happened
   */
  Map<String, AttributeGenerator<?>> resolveAttributeGenerators(
      String tenantName, BiosStreamConfig streamConfig) throws TfosException, ApplicationException;
}
