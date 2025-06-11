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
#ifndef TFOSCSDK_HTTP_RESOURCE_RESOLVER_H_
#define TFOSCSDK_HTTP_RESOURCE_RESOLVER_H_

#include "tfos-physical/resource_resolver.h"

namespace tfos::csdk {

/**
 * Implementation of this class provides resource strings for SDK operations.
 */
class HttpResourceResolver final : public ResourceResolver {
 public:
  const std::string GetMethod(CSdkOperationId id) const;
  const std::string GetResource(CSdkOperationId id, const Resources &resources) const;
  const std::string GetContentType(CSdkOperationId id) const;

 private:
  const std::string Substitute(const char *str, size_t len, const Resources &resources) const;
};

}  // namespace tfos::csdk
#endif  // TFOSCSDK_HTTP_RESOURCE_RESOLVER_H_
