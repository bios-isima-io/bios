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
#ifndef TFOSCSDK_RESOURCE_RESOLVER_H_
#define TFOSCSDK_RESOURCE_RESOLVER_H_

#include <string>

#include "tfoscsdk/csdk.h"

namespace tfos::csdk {

class Resources;

/**
 * Implementation of this class provides resource strings for SDK operations.
 */
class ResourceResolver {
 public:
  virtual ~ResourceResolver() = default;

  /**
   * Resolves operation method for specified operation.
   *
   * The method returns nullptr when the method could not be resolved. The
   * caller must check whether the returned value is nullptr.
   *
   * @return Method string. The implementation must keep the memory of
   *     the string after the call. The caller must not try to delete the
   * returned string.
   */
  virtual const std::string GetMethod(CSdkOperationId id) const = 0;
  /**
   * Resolves resource for specified operation.
   *
   * The method returns nullptr when the method could not be resolved. The
   * caller must check whether the returned value is nullptr.
   *
   * @return Resource string. The implementation must keep the memory of
   *     the string after the call. The caller must not try to delete the
   * returned string.
   */
  virtual const std::string GetResource(CSdkOperationId id, const Resources &resources) const = 0;
  /**
   * Resolves content type based on operation.
   *
   * @param id operation id
   * @return Content type string
   */
  virtual const std::string GetContentType(CSdkOperationId id) const = 0;
};

}  // namespace tfos::csdk
#endif  // TFOSCSDK_RESOURCE_RESOLVER_H_
