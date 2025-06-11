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

/**
 * The password manager class to abstract the implementation of password key generation from the
 * interface.
 */
package io.isima.bios.auth.v1.impl;

import org.mindrot.jbcrypt.BCrypt;

public final class PasswordManager {

  /** Rounds of encryption. */
  private int rounds;

  /**
   * Creates a password manager with number of rounds of encryption.
   *
   * @param rounds the complexity of hashing.
   */
  public PasswordManager(int rounds) {
    this.rounds = rounds;
  }

  /**
   * Creates password manager with default rounds. Rounds is all about complexity of the password
   * encryption. We need to set it so that each password key derivation takes at least 100 ms.
   */
  public PasswordManager() {
    this.rounds = 11;
  }

  public String encode(String password) {
    return BCrypt.hashpw(password, BCrypt.gensalt(rounds));
  }

  /**
   * Check password with key.
   *
   * @param password plain text password to check
   * @param key encoded key to check with
   * @return True if password matches key, else false.
   */
  public Boolean check(String password, String key) {

    try {
      return BCrypt.checkpw(password, key);
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  public void setRounds(int rounds) {
    this.rounds = rounds;
  }

  public int getRounds() {
    return this.rounds;
  }
}
