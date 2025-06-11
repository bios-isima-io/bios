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
package io.isima.bios.admin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * State keeper that is useful to validate a structured object.
 *
 * <p>This tracker is designed to keep states of structured object validation in DFS manner. The
 * class provides following features:
 *
 * <ul>
 *   <li>Keeps the names of validation target for each level (paths)
 *   <li>Keeps the parameters collected during the validation (such as tenantName, signalName, etc.)
 *   <li>Keeps set of names in multiple categories. The class user can (and must) define the
 *       categories
 * </ul>
 *
 * <p>The user should clone the instance by using method {@link #next} when the verification goes to
 * the next level. Cloned instance inherits all states of the original instance. Modifying the state
 * in the cloned instance does not affect the original except pools of names.
 */
public class ValidationState {
  // Used for keeping the context of the validation
  private final Map<String, String> context;

  // Used for tracking current position in the structured validation target
  private final List<String> paths;

  // A map of category and set of names used for checking name conflicts or references.
  // The object is global (common among all verification layers), i.e., change to this object in a
  // cloned state affects the parent. If a localized pool of names is necessary, manage the names
  // outside this object.
  private final Map<String, Set<String>> namesInCategories;

  public ValidationState(String initialPath) {
    context = new LinkedHashMap<>();
    paths = new ArrayList<>();
    paths.add(initialPath);
    namesInCategories = new HashMap<>();
  }

  private ValidationState(ValidationState prev, String newPath) {
    this.context = new LinkedHashMap<>(prev.context);
    paths = new ArrayList<>(prev.paths);
    paths.add(newPath);
    namesInCategories = prev.namesInCategories;
  }

  /**
   * Creates a next level of validation.
   *
   * <p>The method clones the current state, put the new path to the state, and return it.
   *
   * <p>Modifying the cloned state does not affect the original.
   *
   * @param newPath The path name of the new verification level.
   * @return Cloned ValidationState instance for the next level.
   */
  public ValidationState next(String newPath) {
    return new ValidationState(this, newPath);
  }

  public ValidationState nextWithIndex(String collectionName, int index) {
    return next(Objects.requireNonNull(collectionName) + "[" + index + "]");
  }

  public String getPaths() {
    return String.join(".", paths);
  }

  public String getCurrentPath() {
    return paths.get(paths.size() - 1);
  }

  public void putContext(String name, String value) {
    Objects.requireNonNull(name);
    Objects.requireNonNull(value);
    context.put(name, value);
  }

  public Map<String, String> getContext() {
    return Collections.unmodifiableMap(context);
  }

  public Set<String> getNames(String category) {
    Objects.requireNonNull(category);
    Set<String> names = namesInCategories.get(category);
    if (names == null) {
      names = new HashSet<>();
      namesInCategories.put(category, names);
    }
    return names;
  }

  /** Build an error message with context. */
  public String buildMessage(String format, Object... params) {
    return buildMessage(String.format(format, params));
  }

  /**
   * Builds an error message with context.
   *
   * @param message Error message
   * @return
   */
  public String buildMessage(String message) {
    final var sb = new StringBuilder(getPaths()).append(": ").append(message);
    final var context = getContext();
    if (!context.isEmpty()) {
      sb.append("; ")
          .append(
              context.entrySet().stream()
                  .map((entry) -> entry.getKey() + "=" + entry.getValue())
                  .collect(Collectors.joining(", ")));
    }
    return sb.toString();
  }
}
