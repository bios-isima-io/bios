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

import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;
import io.isima.bios.common.Constants;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ContextOpState;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Ip2GeoContextHandler implements GlobalContextHandler {
  private static final Logger logger = LoggerFactory.getLogger(Ip2GeoContextHandler.class);

  private boolean initialized = false;

  private DatabaseReader reader = null;

  public static final String IP2GEO_CONTEXT = Constants.PREFIX_INTERNAL_NAME + "ip2geo";

  private final List<String> attributesToEnrich =
      Arrays.asList(
          "city",
          "geoNameId",
          "country",
          "countryIsoCode",
          "isInEuropeanUnion",
          "continent",
          "continentCode",
          "latitude",
          "longitude",
          "timezone",
          "metroCode",
          "averageIncome",
          "populationDensity",
          "accuracyRadius",
          "postalCode",
          "userType",
          "subdivisionName",
          "subdivisionCode",
          "autonomousSystemNumber",
          "isp",
          "domain",
          "autonomousSystemOrganization",
          "connectionType",
          "organization",
          "userCount",
          "staticIpScore",
          "isAnonymous",
          "isAnonymousVpn",
          "isHostingProvider",
          "isLegitimateProxy",
          "isPublicProxy",
          "isResidentialProxy",
          "isTorExitNode");

  public Ip2GeoContextHandler() {
    try (InputStream inputStream =
        Thread.currentThread().getContextClassLoader().getResourceAsStream("GeoLite2-City.mmdb")) {
      reader = new DatabaseReader.Builder(inputStream).withCache(new CHMCache()).build();
      initialized = true;
    } catch (IOException ioe) {
      logger.error("Ip2Geo init failed. {}", ioe);
    }
  }

  private String defaultStringIfNull(String strVal) {
    if (null == strVal) {
      return "MISSING";
    }
    return strVal;
  }

  private Long defaultIntegerIfNull(Integer intVal) {
    if (null == intVal) {
      return 0L;
    }
    return Long.valueOf(intVal);
  }

  private Double defaultDoubleIfNull(Double decVal) {
    if (null == decVal) {
      return 0.0;
    }
    return decVal;
  }

  private Boolean defaultBooleanIfNull(Boolean boolVal) {
    if (null == boolVal) {
      return false;
    }
    return boolVal;
  }

  private String sanitize(String ipOrHostname) {
    return ipOrHostname.split(",")[0].replace("\"", "").strip();
  }

  @Override
  public Event getContextEntry(final List<Object> key) {
    Event event = new EventJson();
    CityResponse cityResponse = null;
    try {
      InetAddress ipAddress = InetAddress.getByName(sanitize(key.get(0).toString()));
      event.set("ipAddress", ipAddress.getHostAddress());
      cityResponse = reader.city(ipAddress);
    } catch (Exception e) {
      logger.debug("Exception during ip2geo resolution: Lookup failed.", e);
      for (String attribute : attributesToEnrich) {
        event.set(attribute, null);
      }
      return event;
    }
    for (String attribute : attributesToEnrich) {
      if (!initialized) {
        event.set(attribute, null);
        continue;
      }
      switch (attribute) {
        case "city":
          event.set(attribute, defaultStringIfNull(cityResponse.getCity().getName()));
          break;
        case "geoNameId":
          event.set(attribute, defaultIntegerIfNull(cityResponse.getCity().getGeoNameId()));
          break;
        case "country":
          event.set(attribute, defaultStringIfNull(cityResponse.getCountry().getName()));
          break;
        case "countryIsoCode":
          event.set(attribute, defaultStringIfNull(cityResponse.getCountry().getIsoCode()));
          break;
        case "isInEuropeanUnion":
          event.set(attribute, defaultBooleanIfNull(cityResponse.getCountry().isInEuropeanUnion()));
          break;
        case "continent":
          event.set(attribute, defaultStringIfNull(cityResponse.getContinent().getName()));
          break;
        case "continentCode":
          event.set(attribute, defaultStringIfNull(cityResponse.getContinent().getCode()));
          break;
        case "latitude":
          event.set(attribute, defaultDoubleIfNull(cityResponse.getLocation().getLatitude()));
          break;
        case "longitude":
          event.set(attribute, defaultDoubleIfNull(cityResponse.getLocation().getLongitude()));
          break;
        case "timezone":
          event.set(attribute, defaultStringIfNull(cityResponse.getLocation().getTimeZone()));
          break;
        case "metroCode":
          event.set(attribute, defaultIntegerIfNull(cityResponse.getLocation().getMetroCode()));
          break;
        case "averageIncome":
          event.set(attribute, defaultIntegerIfNull(cityResponse.getLocation().getAverageIncome()));
          break;
        case "populationDensity":
          event.set(
              attribute, defaultIntegerIfNull(cityResponse.getLocation().getPopulationDensity()));
          break;
        case "accuracyRadius":
          event.set(
              attribute, defaultIntegerIfNull(cityResponse.getLocation().getAccuracyRadius()));
          break;
        case "postalCode":
          event.set(attribute, defaultStringIfNull(cityResponse.getPostal().getCode()));
          break;
        case "subdivisionName":
          event.set(
              attribute, defaultStringIfNull(cityResponse.getMostSpecificSubdivision().getName()));
          break;
        case "subdivisionCode":
          event.set(
              attribute,
              defaultStringIfNull(cityResponse.getMostSpecificSubdivision().getIsoCode()));
          break;
        case "autonomousSystemNumber":
          event.set(
              attribute,
              defaultIntegerIfNull(cityResponse.getTraits().getAutonomousSystemNumber()));
          break;
        case "autonomousSystemOrganization":
          event.set(
              attribute,
              defaultStringIfNull(cityResponse.getTraits().getAutonomousSystemOrganization()));
          break;
        case "connectionType":
          if (cityResponse.getTraits().getConnectionType() != null) {
            event.set(attribute, cityResponse.getTraits().getConnectionType().toString());
          } else {
            event.set(attribute, "");
          }
          break;
        case "isp":
          event.set(attribute, defaultStringIfNull(cityResponse.getTraits().getIsp()));
          break;
        case "domain":
          event.set(attribute, defaultStringIfNull(cityResponse.getTraits().getDomain()));
          break;
        case "organization":
          event.set(attribute, defaultStringIfNull(cityResponse.getTraits().getOrganization()));
          break;
        case "staticIpScore":
          event.set(attribute, defaultDoubleIfNull(cityResponse.getTraits().getStaticIpScore()));
          break;
        case "userCount":
          event.set(attribute, defaultIntegerIfNull(cityResponse.getTraits().getUserCount()));
          break;
        case "userType":
          event.set(attribute, defaultStringIfNull(cityResponse.getTraits().getUserType()));
          break;
        case "isAnonymous":
          event.set(attribute, defaultBooleanIfNull(cityResponse.getTraits().isAnonymous()));
          break;
        case "isAnonymousVpn":
          event.set(attribute, defaultBooleanIfNull(cityResponse.getTraits().isAnonymousVpn()));
          break;
        case "isHostingProvider":
          event.set(attribute, defaultBooleanIfNull(cityResponse.getTraits().isHostingProvider()));
          break;
        case "isLegitimateProxy":
          event.set(attribute, defaultBooleanIfNull(cityResponse.getTraits().isLegitimateProxy()));
          break;
        case "isPublicProxy":
          event.set(attribute, defaultBooleanIfNull(cityResponse.getTraits().isPublicProxy()));
          break;
        case "isResidentialProxy":
          event.set(attribute, defaultBooleanIfNull(cityResponse.getTraits().isResidentialProxy()));
          break;
        case "isTorExitNode":
          event.set(attribute, defaultBooleanIfNull(cityResponse.getTraits().isTorExitNode()));
          break;
        default:
          logger.warn("Unknown attribute encountered: {}", attribute);
          event.set(attribute, "");
          break;
      }
    }
    return event;
  }

  @Override
  public CompletionStage<Event> getContextEntryAsync(ExecutionState state, List<Object> key) {
    // TODO(BIOS-3918): Check if this staging is really necessary
    return CompletableFuture.supplyAsync(() -> getContextEntry(key), state.getExecutor());
  }

  @Override
  public List<Event> getContextEntries(List<List<Object>> keys)
      throws TfosException, ApplicationException {
    List<Event> events = new ArrayList<>();
    for (List<Object> key : keys) {
      Event event = getContextEntry(key);
      event.setIngestTimestamp(new Date());
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
