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

import io.isima.bios.models.Duplicatable;
import io.isima.bios.models.MissingLookupPolicy;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * One instance of this class corresponds to an entry of "enrichments" in Context/Stream
 * Configuration JSON. This stores pre-compiled config for quick access during data plane operations
 * such as getting context entries.
 */
@ToString
@Getter
@Setter
public class CompiledEnrichment extends Duplicatable {

  protected EnrichmentKind enrichmentKind;

  protected List<String> foreignKey;

  protected List<CompiledAttribute> compiledAttributes;

  /** For SIMPLE_VALUE kind of enrichments, the context this enrichment refers to. */
  protected StreamDesc joiningContext;

  protected MissingLookupPolicy missingLookupPolicy;

  protected Object fillIn;

  public void addCompiledAttribute(CompiledAttribute compiledAttribute) {
    if (compiledAttributes == null) {
      compiledAttributes = new ArrayList<>();
    }
    compiledAttributes.add(compiledAttribute);
  }

  @Override
  public CompiledEnrichment duplicate() {
    final CompiledEnrichment clone = new CompiledEnrichment();
    clone.joiningContext = joiningContext;
    if (foreignKey != null) {
      clone.foreignKey = new ArrayList<>(foreignKey);
    }
    clone.compiledAttributes = copyList(compiledAttributes);
    return clone;
  }
}
