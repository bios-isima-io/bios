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
package io.isima.bios.admin.v1.impl;

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.StreamConfig;

public class AdminImplUtils {
  /**
   * Subroutine to generate an exception message with parameters.
   *
   * @return Error message string builder
   */
  static StringBuilder createExceptionMessage(final String message, final TenantDesc tenantDesc) {
    StringBuilder sb = new StringBuilder(message).append("; tenant=").append(tenantDesc.getName());
    return sb;
  }

  /**
   * Subroutine to generate an exception message with parameters.
   *
   * @return Error message string builder
   */
  static StringBuilder createExceptionMessage(
      final String message, final TenantDesc tenantDesc, final StreamConfig streamConfig) {
    return createExceptionMessage(message, tenantDesc)
        .append(", ")
        .append(streamConfig.getType().name().toLowerCase())
        .append("=")
        .append(streamConfig.getName());
  }

  static StringBuilder createExceptionMessage(
      final String message,
      final TenantDesc tenantDesc,
      final StreamConfig streamConfig,
      final AttributeDesc attr) {
    return createExceptionMessage(message, tenantDesc, streamConfig)
        .append(", attribute=")
        .append(attr.getName());
  }

  static void sanityCheckStringItem(
      TenantDesc tenantDesc, StreamDesc streamDesc, String attributeName, String listName)
      throws ConstraintViolationException {
    if (attributeName == null || attributeName.isEmpty()) {
      throw new ConstraintViolationException(
          createExceptionMessage(
                  listName + " may not contain null or empty string", tenantDesc, streamDesc)
              .toString());
    }
  }

  static AttributeDesc findAttributeReference(
      TenantDesc tenantDesc,
      StreamDesc streamDesc,
      String attribute,
      boolean checkInAdditionalAttributes,
      String message)
      throws ConstraintViolationException {
    for (AttributeDesc attribDesc : streamDesc.getAttributes()) {
      if (attribDesc.getName().equalsIgnoreCase(attribute)) {
        return attribDesc;
      }
    }
    if (checkInAdditionalAttributes && streamDesc.getAdditionalAttributes() != null) {
      for (AttributeDesc attribDesc : streamDesc.getAdditionalAttributes()) {
        if (attribDesc.getName().equalsIgnoreCase(attribute)) {
          return attribDesc;
        }
      }
    }
    throw new ConstraintViolationException(
        createExceptionMessage(
                message + " must contain stream or preprocess attributes", tenantDesc, streamDesc)
            .append(", attribute=")
            .append(attribute)
            .toString());
  }
}
