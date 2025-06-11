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
package io.isima.bios.service.handler;

import com.opencsv.CSVReader;
import io.isima.bios.common.Constants;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ContextOpState;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IpBlacklistContextHandler implements GlobalContextHandler {
  private static final Logger logger = LoggerFactory.getLogger(IpBlacklistContextHandler.class);

  private boolean initialized = false;

  private Map<String, Event> blockedIpAddressMap = new HashMap<>();

  public static final String IPBLACKLIST_CONTEXT = Constants.PREFIX_INTERNAL_NAME + "ipBlacklist";

  public IpBlacklistContextHandler() {
    String blockedIpAddressFile = TfosConfig.getBlockedIpAddressesFile();
    if (blockedIpAddressFile == null) {
      return;
    }
    if (Files.isRegularFile(Paths.get(blockedIpAddressFile))) {
      try (final var reader = new CSVReader(new FileReader(blockedIpAddressFile))) {
        List<String[]> records = reader.readAll();
        for (String[] record : records) {
          Event event = new EventJson();
          String ipAddress = record[0];
          event.set("ipAddress", ipAddress);
          event.set("clusterName", record[1]);
          event.set("cloudProvider", record[2]);
          event.set("region", record[3]);
          event.set("numViolations", Integer.valueOf(record[4]));
          event.setIngestTimestamp(new Date());
          blockedIpAddressMap.put(ipAddress, event);
        }
        logger.info("Initialized IP blacklist with file: {}", blockedIpAddressFile);
        initialized = true;
      } catch (Exception e) {
        logger.error("Initialization failure: {}", e);
      }
    }
  }

  @Override
  public Event getContextEntry(final List<Object> key) {
    if (!initialized) {
      return null;
    }
    return blockedIpAddressMap.get(key.get(0));
  }

  @Override
  public CompletionStage<Event> getContextEntryAsync(ExecutionState state, List<Object> key) {
    return CompletableFuture.supplyAsync(() -> getContextEntry(key), state.getExecutor());
  }

  @Override
  public List<Event> getContextEntries(List<List<Object>> keys)
      throws TfosException, ApplicationException {
    List<Event> events = new ArrayList<>();
    for (List<Object> key : keys) {
      Event event = getContextEntry(key);
      if (event != null) {
        events.add(event);
      }
    }
    return events;
  }

  @Override
  public void getContextEntriesAsync(
      List<List<Object>> keys,
      ContextOpState state,
      Consumer<List<Event>> acceptor,
      Consumer<Throwable> errorHandler) {
    try {
      List<Event> events = getContextEntries(keys);
      acceptor.accept(events);
    } catch (Throwable t) {
      errorHandler.accept(t);
    }
  }
}
