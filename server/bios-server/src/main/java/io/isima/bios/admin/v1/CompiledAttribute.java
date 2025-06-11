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
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * One instance of this class corresponds to an entry of "enrichedAttribute" inside an enrichment in
 * Context/Stream Configuration JSON. This stores pre-compiled config for quick access during data
 * plane operations such as getting context entries.
 */
@ToString
@Getter
@Setter
public class CompiledAttribute extends Duplicatable {

  /**
   * Name of the attribute as referred to by its users. The name of this attribute is determined by:
   *
   * <ul>
   *   <li>"as" if it is set. This is required for valuePickFirst and optional for value.
   *   <li>else, the second part of "value", which is in the form "context_name.attribute_name".
   * </ul>
   */
  protected String aliasedName;

  /** For SIMPLE_VALUE enrichments, the attribute name this enrichedAttribute refers to . */
  protected String joinedAttributeName;

  /** For VALUE_PICK_FIRST enrichments, the list of contexts this enrichedAttribute refers to. */
  protected List<StreamDesc> candidateContexts;

  /**
   * For VALUE_PICK_FIRST enrichments, the list of attribute names to pick from the above contexts.
   */
  protected List<String> candidateAttributeNames;

  protected Object fillIn;

  public void addCandidateContext(StreamDesc candidateContext) {
    if (this.candidateContexts == null) {
      this.candidateContexts = new ArrayList<>();
    }
    this.candidateContexts.add(candidateContext);
  }

  public void addCandidateAttributeName(String joinedAttributeName) {
    if (this.candidateAttributeNames == null) {
      this.candidateAttributeNames = new ArrayList<>();
    }
    this.candidateAttributeNames.add(joinedAttributeName);
  }

  @Override
  public CompiledAttribute duplicate() {
    final CompiledAttribute clone = new CompiledAttribute();
    clone.aliasedName = aliasedName;
    clone.joinedAttributeName = joinedAttributeName;
    clone.candidateContexts = copyList(candidateContexts);
    if (candidateAttributeNames != null) {
      clone.candidateAttributeNames = new ArrayList<>(candidateAttributeNames);
    }
    return clone;
  }
}
