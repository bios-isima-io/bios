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
package io.isima.bios.models;

import static org.junit.Assert.assertEquals;

import com.google.common.net.InetAddresses;
import io.isima.bios.errors.exception.InvalidValueSyntaxException;
import io.isima.bios.models.v1.InternalAttributeType;
import java.math.BigInteger;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class InternalAttributeTypeTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {}

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testStringParserThreadSafety() throws InterruptedException, ExecutionException {
    final Random random = new Random();
    testParserThreadSafety(
        InternalAttributeType.STRING, () -> Integer.toString(random.nextInt()), orig -> orig);
  }

  @Test
  public void testBooleanParserThreadSafety() throws InterruptedException, ExecutionException {
    final Random random = new Random();
    testParserThreadSafety(
        InternalAttributeType.BOOLEAN, () -> random.nextBoolean(), orig -> orig.toString());
  }

  @Test
  public void testIntParserThreadSafety() throws InterruptedException, ExecutionException {
    final Random random = new Random();
    testParserThreadSafety(
        InternalAttributeType.INT, () -> random.nextInt(), orig -> orig.toString());
  }

  @Test
  public void testNumberParserThreadSafety() throws InterruptedException, ExecutionException {
    testParserThreadSafety(
        InternalAttributeType.NUMBER,
        () -> rndBigInt(new BigInteger("18446744073709551615000000")),
        orig -> orig.toString());
  }

  private static BigInteger rndBigInt(BigInteger max) {
    Random rnd = new Random();
    do {
      BigInteger i = new BigInteger(max.bitLength(), rnd);
      if (i.compareTo(max) <= 0) {
        return i;
      }
    } while (true);
  }

  @Test
  public void testDoubleParserThreadSafety() throws InterruptedException, ExecutionException {
    final Random random = new Random();
    testParserThreadSafety(
        InternalAttributeType.DOUBLE, () -> random.nextDouble(), orig -> orig.toString());
  }

  @Test
  public void testInetParserThreadSafety() throws InterruptedException, ExecutionException {
    final Random random = new Random();
    testParserThreadSafety(
        InternalAttributeType.INET,
        () -> InetAddresses.fromInteger(random.nextInt()),
        orig -> InetAddresses.toAddrString(orig));
  }

  @Test
  public void testDateParserThreadSafety() throws InterruptedException, ExecutionException {
    testParserThreadSafety(
        InternalAttributeType.DATE, () -> LocalDate.now(), orig -> orig.toString());
  }

  @Test
  public void testTimestampParserThreadSafety() throws InterruptedException, ExecutionException {
    testParserThreadSafety(
        InternalAttributeType.TIMESTAMP, () -> new Date(), orig -> getIsoDate(orig));
  }

  private String getIsoDate(final Date date) {
    final DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    df.setTimeZone(TimeZone.getTimeZone("UTC"));
    return df.format(date);
  }

  @Test
  public void testUuidParserThreadSafety() throws InterruptedException, ExecutionException {
    testParserThreadSafety(
        InternalAttributeType.UUID, () -> UUID.randomUUID(), orig -> orig.toString());
  }

  private <T> void testParserThreadSafety(
      final InternalAttributeType attributeType,
      final Supplier<T> sourceSupplier,
      final Function<T, String> convertToString)
      throws InterruptedException, ExecutionException {
    final int concurrency = 40;
    final List<T> sources = new ArrayList<>();
    final List<CompletableFuture<Object>> futures = new ArrayList<>();
    for (int i = 0; i < concurrency; ++i) {
      final T source = sourceSupplier.get();
      sources.add(source);
      futures.add(
          CompletableFuture.supplyAsync(() -> parse(attributeType, convertToString.apply(source))));
    }
    for (int i = 0; i < concurrency; ++i) {
      Object parsed = futures.get(i).get();
      assertEquals(sources.get(i), parsed);
    }
  }

  private static Object parse(InternalAttributeType attributeType, String src) {
    try {
      return attributeType.parse(src);
    } catch (InvalidValueSyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}
