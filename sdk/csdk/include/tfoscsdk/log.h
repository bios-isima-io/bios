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
#ifndef TFOS_CSDK_LOG_H_
#define TFOS_CSDK_LOG_H_

/**
 * Use this include file to import fundamental C-SDK definitions into an
 * implementation file. Another include file should not include this file.
 */

// Enable debug print only for debug build.
#if !defined(NDEBUG)
#define DEBUG_LOG_ENABLED
#endif

#ifdef DEBUG_LOG_ENABLED
#include <libgen.h>
#include <pthread.h>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#define TFOS_DEBUG_LOG_(format, ...)                                            \
  do {                                                                          \
    fprintf(stderr, "(%d) [%lx] %s:%d: " format "\n", getpid(), pthread_self(), \
            basename((char *)__FILE__),  __LINE__, ##__VA_ARGS__);              \
  } while (0)

#if defined(DEBUG_LOG_ENABLED)
#define DEBUG_LOG(format, ...) TFOS_DEBUG_LOG_(format, ##__VA_ARGS__)
#else
#define DEBUG_LOG(format, ...)
#endif

#define TEST_LOG(format, ...) TFOS_DEBUG_LOG_(format, ##__VA_ARGS__)

#endif  // TFOS_CSDK_LOG_H_
