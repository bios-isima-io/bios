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
package io.isima.bios.load.utils;

import static io.isima.bios.sdk.Bios.keys;

import io.isima.bios.models.AppType;
import io.isima.bios.models.isql.ISqlResponse;
import io.isima.bios.sdk.Bios;
import io.isima.bios.sdk.Session;
import io.isima.bios.sdk.exceptions.BiosClientException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

public class DbUtils {
  private static Logger logger = Logger.getLogger(DbUtils.class);
  private static final String SSL_CERT_FILE = "SSL_CERT_FILE";

  public static Session initSession(String host, int port, String email, String password)
      throws BiosClientException {
    String sslCertFile = null;
    if (System.getProperty(SSL_CERT_FILE) != null) {
      final var certFilePath = Paths.get(System.getProperty(SSL_CERT_FILE));
      if (Files.exists(certFilePath)) {
        sslCertFile = System.getProperty(SSL_CERT_FILE);
      }
    }
    Session.Starter builder = Bios.newSession(host).port(port)
        .user(email)
        .password(password)
        .sslCertFile(sslCertFile)
        .appName("load-gen")
        .appType(AppType.REALTIME);
    return builder.connect();
  }

  public static ISqlResponse insert(Session session, String signal, String event)
      throws BiosClientException {
    return session.execute(Bios.isql().insert()
        .into(signal)
        .csv(event)
        .build());
  }

  public static ISqlResponse insertBulk(Session session, String signal, List<String> events)
      throws BiosClientException {
    return session.execute(Bios.isql().insert()
        .into(signal)
        .csvBulk(events)
        .build());
  }

  public static ISqlResponse select(Session session, String signal, long start, long delta)
      throws BiosClientException {
    return session.execute(Bios.isql().select()
        .fromSignal(signal)
        .timeRange(start, delta)
        .build());
  }

  public static ISqlResponse selectGB(Session session, String signal, long start, long delta,
      String[] attributes) throws BiosClientException {
    return session.execute(Bios.isql().select()
        .fromSignal(signal)
        .groupBy(attributes)
        .timeRange(start, Duration.ofMillis(delta))
        .build());
  }

  public static ISqlResponse getContextEntries(Session session, String context, Comparable... keys)
      throws BiosClientException {
    return session.execute(Bios.isql().select()
        .fromContext(context)
        .where(keys().in(keys))
        .build());
  }

  public static void putContextEntries(Session session, String context, List<String> entries)
      throws BiosClientException {
    session.execute(Bios.isql().upsert()
        .intoContext(context)
        .csvBulk(entries)
        .build());
  }

  public static void updateContextEntry(Session session, String context, String key,
      Map<String, String> attrMap) throws BiosClientException {
    session.execute(Bios.isql().updateContext(context)
        .set(Collections.unmodifiableMap(attrMap))
        .where(keys().eq(key))
        .build());
  }

  public static void deleteContextEntries(Session session, String context, List<Object> entries)
      throws BiosClientException {
    session.execute(Bios.isql().delete()
        .fromContext(context)
        .where(keys().in(entries))
        .build());
  }

  public static Session renew(Session session, String host, int port, String email, String password)
      throws BiosClientException {
    try {
      session.close();
      Session.Starter builder = Bios.newSession(host).port(port)
          .user(email)
          .password(password);
      session = builder.connect();
    } catch (Exception e) {
      logger.warn(e);
    }
    return session;

  }

}
