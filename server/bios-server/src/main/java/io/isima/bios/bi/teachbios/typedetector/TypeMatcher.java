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
package io.isima.bios.bi.teachbios.typedetector;

/*
This class attempts to cast a given value into a data type and returns true if the cast was a
success and false otherwise
 */
public abstract class TypeMatcher {

  public abstract boolean tryParseAsInteger(String value);

  public abstract boolean tryParseAsDouble(String value);

  public abstract boolean tryParseAsBoolean(String value);
}
