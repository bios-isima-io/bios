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
package io.isima.bios.auth.v1;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.isima.bios.auth.v1.impl.PasswordManager;
import java.security.SecureRandom;
import java.util.Date;
import java.util.Random;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class PasswordManagerTest {

  PasswordManager passwordManager;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {}

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  public int getRandomNumberInRange(int min, int max) {
    Random random = new Random();
    return random.nextInt(max - min) + min;
  }

  private static String genPassword(int length) {

    // ASCII range - alphanumeric (0-9, a-z, A-Z)
    final String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    SecureRandom random = new SecureRandom();
    StringBuilder sb = new StringBuilder();

    // each iteration of loop choose a character randomly from the given ASCII
    // range
    // and append it to StringBuilder instance

    for (int i = 0; i < length; i++) {
      int randomIndex = random.nextInt(chars.length());
      sb.append(chars.charAt(randomIndex));
    }

    return sb.toString();
  }

  @Before
  public void setUp() throws Exception {
    this.passwordManager = new PasswordManager();
  }

  @After
  public void tearDown() throws Exception {}

  /** Test password of any length. */
  @Test
  public void testDecodedPasswordMustMatchForDifferentLengthPasswords() {
    int rounds = passwordManager.getRounds();
    passwordManager.setRounds(4);
    for (int i = 0; i < 200; i++) {
      String password = genPassword(getRandomNumberInRange(4, 256));
      String key = passwordManager.encode(password);
      assertTrue(passwordManager.check(password, key));
    }
    passwordManager.setRounds(rounds);
  }

  /**
   * This test is to make password manager future proof with improving hardware. We ideally want the
   * password encoding to take at least 100ms. If hardware improves and this test, which is
   * currently taking about 200ms fails, then increase the rounds parameter so that the test passes.
   */
  @Test
  public void testPasswordEncodingShouldTakeAtleastHundredMilliSeconds() {
    String password = genPassword(32);
    Date date = new Date();
    long startTime = date.getTime();
    String key = passwordManager.encode(password);
    date = new Date();
    long endTime = date.getTime();
    long encodingTime = endTime - startTime;
    assertTrue(
        "Encoding time: " + encodingTime + "Should be greater than 100ms", encodingTime > 100);
    assertTrue(passwordManager.check(password, key));
  }

  /** Test password manager constructor with rounds specified. */
  @Test
  public void testPasswordManagerWithRounds() {
    PasswordManager pwm = new PasswordManager(8);
    String password = genPassword(48);
    String key = pwm.encode(password);
    assertTrue(pwm.check(password, key));
  }

  /**
   * Test that encoding has all the information to decode and the rounds set in one password manager
   * should not fail password encoded by another password manager.
   */
  @Test
  public void testPasswordKeyShouldBeIndependentOfInternals() {
    PasswordManager pwm = new PasswordManager(4);
    String password = genPassword(64);
    String key = pwm.encode(password);
    assertTrue(passwordManager.check(password, key));
  }

  /**
   * Test that using old password should not cause exception password encoded by another password
   * manager.
   */
  @Test
  public void testInvalidPasswordShouldNotCauseException() {
    String password = genPassword(64);
    String key = "0cd2a134e14ec1bf03132c91ce6e06ee1c55a269";
    assertFalse(passwordManager.check(password, key));
  }
}
