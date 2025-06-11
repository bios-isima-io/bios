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
package io.isima.bios.data.impl;

import io.isima.bios.service.handler.GlobalContextHandler;
import io.isima.bios.service.handler.Ip2GeoContextHandler;
import io.isima.bios.service.handler.IpBlacklistContextHandler;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class GlobalContextRepository {

  private static GlobalContextRepository instance;

  /**
   * Returns an instacne of this class.
   *
   * <p>The method is not thread safe assuming that it is called during server bootstrap in a single
   * thread for the first time.
   */
  public static GlobalContextRepository getInstance() {
    if (instance == null) {
      instance = new GlobalContextRepository();
    }
    return instance;
  }

  private final Map<String, GlobalContextHandler> globalContextHandlers = new ConcurrentHashMap<>();

  private GlobalContextRepository() {
    globalContextHandlers.put(Ip2GeoContextHandler.IP2GEO_CONTEXT, new Ip2GeoContextHandler());
    globalContextHandlers.put(
        IpBlacklistContextHandler.IPBLACKLIST_CONTEXT, new IpBlacklistContextHandler());
  }

  public GlobalContextHandler getContextHandler(String streamName) {
    return globalContextHandlers.get(streamName);
  }
}
