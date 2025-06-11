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
#ifndef TFOSCSDK_HTTP_CONSTANTS_H_
#define TFOSCSDK_HTTP_CONSTANTS_H_

#include <string>

namespace tfos::csdk {

// nghttp2 special headers
#define kHeaderNameMethod ":method"
#define kHeaderNameScheme ":scheme"
#define kHeaderNameAuthority ":authority"
#define kHeaderNamePath ":path"

// header field names
#define kHeaderNameAuthorization "authorization"
#define kHeaderNameContentType "content-type"
#define kHeaderNameContentLength "content-length"
#define kHeaderNameAccept "accept"
#define kHeaderNameInternalRequest "x-bios-internal-request"
#define kHeaderNameClientVersion "x-bios-client-version"

#define kHeaderValueMetrics "metrics"

// misc.
#define kBearer "Bearer "

}  // namespace tfos::csdk

#endif  // TFOSCSDK_HTTP_CONSTANTS_H_
