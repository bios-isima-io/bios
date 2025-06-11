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
package io.isima.bios.qa.quick;

import static io.isima.bios.sdk.Bios.keys;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assume.assumeTrue;

import io.isima.bios.models.isql.ISqlResponse;
import io.isima.bios.qautils.TestConfig;
import io.isima.bios.sdk.Bios;
import io.isima.bios.sdk.Session;
import io.isima.bios.sdk.Statement;
import io.isima.bios.sdk.errors.BiosClientError;
import io.isima.bios.sdk.exceptions.BiosClientException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.BeforeClass;
import org.junit.Test;

public class AsyncMethodsIT {
  private static final String TENANT_NAME = "biosClientE2eTest";
  private static final String SIGNAL_NAME_BASIC = "basic";

  private static final String CONTEXT_NAME_SIMPLE = "simpleContext";

  private static final String CONTEXT_NAME_CUSTOMER = "customer_context";
  private static final String SIGNAL_NAME_ENRICH_GLOBAL_MVP = "enrich_global_mvp_policy";

  private static String host;
  private static int port;

  @BeforeClass
  public static void setUpBeforeClass() {
    assumeTrue(TestConfig.isIntegrationTest());
    host = TestConfig.getEndpoint();
    port = TestConfig.getPort();
  }

  @Test
  public void testConnectAsync() throws Exception {
    Session session = null;
    try {
      Session.Starter starter =
          Bios.newSession(host).port(port).user("ingest@" + TENANT_NAME).password("ingest");

      final long start = System.nanoTime();
      final var timestamp1 = new AtomicLong();

      final var future =
          starter
              .connectAsync()
              .thenApply(
                  (s) -> {
                    timestamp1.set(System.nanoTime());
                    System.out.println("connected in thread " + Thread.currentThread().getName());
                    return s;
                  });
      final var timestamp2 = System.nanoTime();
      session = future.toCompletableFuture().get();
      assertThat(session, notNullValue());
      assertThat(timestamp1.get(), greaterThan(start));
      assertThat(timestamp1.get(), greaterThan(timestamp2));
    } finally {
      if (session != null) {
        session.close();
      }
    }
  }

  @Test
  public void testConnectionFailure() {
    Session session = null;
    Session.Starter starter =
        Bios.newSession("no-such-host.example.com")
            .port(port)
            .user("ingest@" + TENANT_NAME)
            .password("ingest");

    final var testFuture = new CompletableFuture<Session>();

    starter
        .connectAsync()
        .whenComplete(
            (s, t) -> {
              if (t != null) {
                final var cause = t instanceof CompletionException ? t.getCause() : t;
                testFuture.completeExceptionally(cause);
              } else {
                testFuture.complete(s);
              }
            });

    final var exception = assertThrows(ExecutionException.class, () -> testFuture.get());
    assertThat(exception.getCause(), instanceOf(BiosClientException.class));
    assertThat(
        ((BiosClientException) (exception.getCause())).getCode(),
        is(BiosClientError.SERVER_CONNECTION_FAILURE));
  }

  @Test
  public void testLoginFailure() {
    Session session = null;
    Session.Starter starter =
        Bios.newSession(host).port(port).user("ingest@" + TENANT_NAME).password("badPassword");

    final var testFuture = new CompletableFuture<Session>();

    starter
        .connectAsync()
        .whenComplete(
            (s, t) -> {
              if (t != null) {
                final var cause = t instanceof CompletionException ? t.getCause() : t;
                testFuture.completeExceptionally(cause);
              } else {
                testFuture.complete(s);
              }
            });

    final var exception = assertThrows(ExecutionException.class, () -> testFuture.get());
    assertThat(exception.getCause(), instanceOf(BiosClientException.class));
    assertThat(
        ((BiosClientException) (exception.getCause())).getCode(), is(BiosClientError.UNAUTHORIZED));
  }

  /** Test passes if no crash happens. */
  @Test
  public void testConcurrentSessionCreation() throws Exception {
    for (int i = 0; i < 10; ++i) {
      Session.Starter starterIngest =
          Bios.newSession(host).port(port).user("ingest@" + TENANT_NAME).password("ingest");

      Session.Starter starterExtract =
          Bios.newSession(host).port(port).user("ingest@" + TENANT_NAME).password("ingest");

      Session.Starter starterAdmin =
          Bios.newSession(host).port(port).user("admin@" + TENANT_NAME).password("admin");

      final var sessionIngest = new AtomicReference<Session>();
      final var sessionExtract = new AtomicReference<Session>();
      final var sessionAdmin = new AtomicReference<Session>();

      CompletableFuture.allOf(
              starterIngest
                  .connectAsync()
                  .thenAccept((session) -> sessionIngest.set(session))
                  .toCompletableFuture(),
              starterExtract
                  .connectAsync()
                  .thenAccept((session) -> sessionExtract.set(session))
                  .toCompletableFuture(),
              starterAdmin
                  .connectAsync()
                  .thenAccept((session) -> sessionAdmin.set(session))
                  .toCompletableFuture())
          .get();

      sessionIngest.get().close();
      sessionExtract.get().close();
      sessionAdmin.get().close();
    }
  }

  /**
   * Test closing session twice concurrently which is not ideal usage but can happen.
   *
   * <p>The test passes if no crash happens.
   */
  @Test
  public void testDoubleSessionClosure() throws Exception {
    for (int i = 0; i < 10; ++i) {
      // Session session = null;
      final var session =
          Bios.newSession(host)
              .port(port)
              .user("ingest@" + TENANT_NAME)
              .password("ingest")
              .connect();

      CompletableFuture.allOf(
              CompletableFuture.runAsync(
                  () -> {
                    try {
                      session.close();
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                  }),
              CompletableFuture.runAsync(
                  () -> {
                    try {
                      session.close();
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                  }))
          .get();
    }
  }

  @Test
  public void testSignalAccess() throws Exception {
    try (final var session =
        Bios.newSession(host).port(port).user("admin@" + TENANT_NAME).password("admin").connect()) {
      final var insertStatement =
          Bios.isql().insert().into(SIGNAL_NAME_BASIC).csv("hello,world,99257").build();
      final long start = System.nanoTime();
      final var timestamp1 = new AtomicLong();
      final var timestamp2 = new AtomicLong();
      final var insertResponse = new AtomicReference<ISqlResponse>();
      final var selectResponse = new AtomicReference<ISqlResponse>();

      // Used to block until the operations are done.
      final var testFuture = new CompletableFuture<Void>();

      // execute async operations
      session
          .executeAsync(insertStatement)
          .thenCompose(
              (response) -> {
                timestamp1.set(System.nanoTime());
                insertResponse.set(response);
                final var now = System.currentTimeMillis();
                final var delta = response.getRecords().get(0).getTimestamp() - now;
                final var statement =
                    Bios.isql()
                        .select()
                        .fromSignal(SIGNAL_NAME_BASIC)
                        .timeRange(now, delta)
                        .build();
                return session.executeAsync(statement);
              })
          .whenComplete(
              (response, t) -> {
                if (t != null) {
                  final var cause = t instanceof CompletionException ? t.getCause() : t;
                  testFuture.completeExceptionally(cause);
                } else {
                  timestamp2.set(System.nanoTime());
                  selectResponse.set(response);
                  testFuture.complete(null);
                }
              });

      final var timestamp3 = System.nanoTime();

      // wait for the async operations finish
      testFuture.get();

      // Check timestamps
      assertThat(timestamp2.get(), greaterThan(timestamp1.get()));
      assertThat(timestamp1.get(), greaterThan(start));
      assertThat(timestamp1.get(), greaterThan(timestamp3));

      System.out.println(insertResponse.get());
      System.out.println(selectResponse.get().getDataWindows().get(0));

      assertThat(selectResponse.get().getDataWindows().size(), is(1));
      assertThat(selectResponse.get().getDataWindows().get(0).getRecords().size(), is(1));
      final var record = selectResponse.get().getDataWindows().get(0).getRecords().get(0);
      assertThat(record.getAttribute("one").asString(), is("hello"));
      assertThat(record.getAttribute("two").asString(), is("world"));
      assertThat(record.getAttribute("three").asLong(), is(99257L));
    }
  }

  @Test
  public void testConcurrentInsertions() throws Exception {
    try (final var session =
        Bios.newSession(host).port(port).user("admin@" + TENANT_NAME).password("admin").connect()) {
      int concurrency = 5;
      var statements = new Statement[concurrency];
      for (int i = 0; i < concurrency; ++i) {
        statements[i] =
            Bios.isql().insert().into(SIGNAL_NAME_BASIC).csv("hello,world," + i).build();
      }

      final var start = System.currentTimeMillis();
      final var future =
          CompletableFuture.allOf(
              session.executeAsync(statements[0]).toCompletableFuture(),
              session.executeAsync(statements[1]).toCompletableFuture(),
              session.executeAsync(statements[2]).toCompletableFuture(),
              session.executeAsync(statements[3]).toCompletableFuture(),
              session.executeAsync(statements[3]).toCompletableFuture());

      future.get();

      final var selectStatement =
          Bios.isql()
              .select()
              .fromSignal(SIGNAL_NAME_BASIC)
              .timeRange(start, System.currentTimeMillis() - start)
              .build();

      final var selectResponse = session.executeAsync(selectStatement).toCompletableFuture().get();

      System.out.println(selectResponse.getDataWindows().get(0));

      assertThat(selectResponse.getDataWindows().size(), is(1));
      assertThat(selectResponse.getDataWindows().get(0).getRecords().size(), is(5));

      for (var record : selectResponse.getDataWindows().get(0).getRecords()) {
        assertThat(record.getAttribute("three").asLong(), in(Set.of(0L, 1L, 2L, 3L, 4L)));
      }
    }
  }

  @Test
  public void testOperationError() throws Exception {
    try (final var session =
        Bios.newSession(host)
            .port(port)
            .user("extract@" + TENANT_NAME)
            .password("extract")
            .connect()) {
      final var insertStatement =
          Bios.isql().insert().into(SIGNAL_NAME_BASIC).csv("hello,world,99257").build();

      // Used to block until the operations are done.
      final var testFuture = new CompletableFuture<Void>();

      // execute async operations
      session
          .executeAsync(insertStatement)
          .whenComplete(
              (response, t) -> {
                if (t != null) {
                  final var cause = t instanceof CompletionException ? t.getCause() : t;
                  testFuture.completeExceptionally(cause);
                } else {
                  testFuture.complete(null);
                }
              });

      // wait for the async operations finish
      final var exception = assertThrows(ExecutionException.class, () -> testFuture.get());
      assertThat(exception.getCause(), instanceOf(BiosClientException.class));
      assertThat(
          ((BiosClientException) exception.getCause()).getCode(), is(BiosClientError.FORBIDDEN));
    }
  }

  @Test
  public void testMultiExecute() throws Exception {
    try (final var session =
        Bios.newSession(host).port(port).user("admin@" + TENANT_NAME).password("admin").connect()) {

      final var testFuture = new CompletableFuture<List<? extends ISqlResponse>>();

      var upsertStatement =
          Bios.isql()
              .upsert()
              .intoContext(CONTEXT_NAME_CUSTOMER)
              .csvBulk(List.of("123,tom,MALE", "456,jane,FEMALE"))
              .build();
      session
          .executeAsync(upsertStatement)
          .thenCompose(
              (response) -> {
                final long start = System.currentTimeMillis();
                final var statement1 =
                    Bios.isql()
                        .insert()
                        .into(SIGNAL_NAME_BASIC)
                        .csvBulk("aaa,bbb,99257", "ccc,ddd,94061", "eee,fff,12345")
                        .build();
                final var future1 = session.executeAsync(statement1).toCompletableFuture();

                final var statement2 =
                    Bios.isql()
                        .insert()
                        .into(SIGNAL_NAME_ENRICH_GLOBAL_MVP)
                        .csvBulk("123,Seattle", "456,Chicago")
                        .build();
                final var future2 = session.executeAsync(statement2).toCompletableFuture();

                return CompletableFuture.allOf(future1, future2)
                    .thenComposeAsync(
                        (none) -> {
                          final long end = System.currentTimeMillis() + 1;
                          final var statement3 =
                              Bios.isql()
                                  .select()
                                  .fromSignal(SIGNAL_NAME_BASIC)
                                  .timeRange(start, end - start)
                                  .build();
                          final var statement4 =
                              Bios.isql()
                                  .select()
                                  .fromSignal(SIGNAL_NAME_ENRICH_GLOBAL_MVP)
                                  .timeRange(start, end - start)
                                  .build();
                          return session.multiExecuteAsync(statement3, statement4);
                        });
              })
          .thenAccept(testFuture::complete)
          .exceptionally(
              (t) -> {
                final var cause = t instanceof CompletionException ? t.getCause() : t;
                testFuture.completeExceptionally(cause);
                return null;
              });

      final var response = testFuture.get();

      assertThat(response.size(), is(2));

      final var dataWindows0 = response.get(0).getDataWindows();
      assertThat(dataWindows0.size(), is(1));
      assertThat(dataWindows0.get(0).getRecords().size(), is(3));

      final var dataWindows1 = response.get(1).getDataWindows();
      assertThat(dataWindows1.size(), is(1));
      assertThat(dataWindows1.get(0).getRecords().size(), is(2));
    }
  }

  @Test
  public void testContextAccess() throws Exception {
    try (final var session =
        Bios.newSession(host).port(port).user("admin@" + TENANT_NAME).password("admin").connect()) {

      final long timestampStart = System.nanoTime();
      final var timestampUpsert = new AtomicLong();
      final var timestampGetEntries = new AtomicLong();
      final var timestampUpdate = new AtomicLong();

      final var getEntriesResponse = new AtomicReference<ISqlResponse>();
      final var selectResponse = new AtomicReference<ISqlResponse>();
      final var getEntriesResponse2 = new AtomicReference<ISqlResponse>();

      // Used to block until the operations are done.
      final var testFuture = new CompletableFuture<Void>();

      // execute async operations
      var upsertStatement =
          Bios.isql()
              .upsert()
              .intoContext(CONTEXT_NAME_SIMPLE)
              .csvBulk(List.of("700,g", "800,h", "900,i", "10000,j"))
              .build();
      session
          .executeAsync(upsertStatement)
          .thenCompose(
              (response) -> {
                timestampUpsert.set(System.nanoTime());

                final var getEntriesStatement =
                    Bios.isql()
                        .select()
                        .fromContext(CONTEXT_NAME_SIMPLE)
                        .where(keys().in("700"))
                        .build();
                final var getEntriesFuture =
                    session
                        .executeAsync(getEntriesStatement)
                        .thenAccept((result) -> getEntriesResponse.set(result));

                final var selectStatement =
                    Bios.isql()
                        .select()
                        .fromContext(CONTEXT_NAME_SIMPLE)
                        .where("the_key = 10000")
                        .build();
                final var selectFuture =
                    session
                        .executeAsync(selectStatement)
                        .thenAccept((result) -> selectResponse.set(result));

                return CompletableFuture.allOf(
                    getEntriesFuture.toCompletableFuture(), selectFuture.toCompletableFuture());
              })
          .thenCompose(
              (none) -> {
                timestampGetEntries.set(System.nanoTime());
                final var updateStatement =
                    Bios.isql()
                        .update(CONTEXT_NAME_SIMPLE)
                        .set(Map.of("the_value", "ZZZ"))
                        .where(keys().in("700"))
                        .build();
                return session.executeAsync(updateStatement);
              })
          .thenCompose(
              (response) -> {
                timestampUpdate.set(System.nanoTime());
                final var getEntriesStatement =
                    Bios.isql()
                        .select()
                        .fromContext(CONTEXT_NAME_SIMPLE)
                        .where(keys().in("700"))
                        .build();
                return session.executeAsync(getEntriesStatement);
              })
          .thenAccept(
              (response) -> {
                getEntriesResponse2.set(response);
                testFuture.complete(null);
              })
          .exceptionally(
              (t) -> {
                final var cause = t instanceof CompletionException ? t.getCause() : t;
                testFuture.completeExceptionally(cause);
                return null;
              });

      final var timestampAfterAsyncCall = System.nanoTime();

      // wait for the async operations finish
      testFuture.get();

      // Check timestamps
      assertThat(timestampUpsert.get(), greaterThan(timestampStart));
      assertThat(timestampGetEntries.get(), greaterThan(timestampUpsert.get()));
      assertThat(timestampUpdate.get(), greaterThan(timestampGetEntries.get()));
      assertThat(timestampUpdate.get(), greaterThan(timestampAfterAsyncCall));

      assertThat(
          getEntriesResponse.get().getRecords().get(0).getAttribute("the_value").asString(),
          is("g"));
      assertThat(
          selectResponse.get().getRecords().get(0).getAttribute("the_value").asString(), is("j"));
      assertThat(
          getEntriesResponse2.get().getRecords().get(0).getAttribute("the_value").asString(),
          is("ZZZ"));
    }
  }
}
