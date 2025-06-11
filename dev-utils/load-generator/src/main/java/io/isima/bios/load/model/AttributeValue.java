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
package io.isima.bios.load.model;

import io.isima.bios.exceptions.InvalidDataRetrievalException;

public interface AttributeValue {
  /**
   * Returns the value as a boolean. If the native type is not boolean, an attempt will be made
   * to convert it to a boolean.
   *
   * @return value as boolean
   * @throws InvalidDataRetrievalException if conversion is not possible and the native type is
   *         not boolean
   */
  boolean asBoolean() throws InvalidDataRetrievalException;

  /**
   * Returns the value as a long. If the native type is not long, an attempt will be made to
   * convert it to boolean.
   *
   * @return value as long
   * @throws InvalidDataRetrievalException if conversion is not possible and the native type is
   *         not long
   */
  long asLong() throws InvalidDataRetrievalException;

  /**
   *
   * Returns the value as a double. If the native type is not double, an attempt will be made to
   * convert it to double.
   *
   * @return value as double
   * @throws InvalidDataRetrievalException if conversion is not possible and the native type is
   *         not double.
   */
  double asDouble() throws InvalidDataRetrievalException;

  /**
   * Returns the value as a byte array. If the native type is not byte array, an attempt will be
   * made to convert it to byte array.
   *
   * @return value as byte array
   * @throws InvalidDataRetrievalException if conversion is not possible and the native type is
   *         not byte array
   */
  byte[] asByteArray() throws InvalidDataRetrievalException;

  /**
   * Returns the value as a String. If the native type is not String, an attempt will be made to
   * convert it to a string.
   *
   * @return value as string
   * @throws InvalidDataRetrievalException If conversion is not possible and the native type is
   *         not string.
   */
  String asString() throws InvalidDataRetrievalException;
}

