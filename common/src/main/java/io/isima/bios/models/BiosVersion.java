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

import java.util.Arrays;

/** Encapsulates version strings and makes them comparable. */
public class BiosVersion implements Comparable<BiosVersion> {
  private static final String EMPTY_VERSION = "0.0.0";
  private static final int MAX_DOTS_IN_VERSION = 4;

  private final String versionString;
  private final int[] versionSequences;
  private final String snapshot;

  /**
   * Constructs Bios version object that is comparable to other bios versions.
   *
   * <p>Accepts nulls. Nulls are treated as version "0.0.0", that is the starting version.
   *
   * @param version version string, can be null
   */
  public BiosVersion(String version) {
    this.versionString = (version == null || version.isEmpty()) ? EMPTY_VERSION : version;
    this.versionSequences = new int[MAX_DOTS_IN_VERSION + 1];
    this.snapshot = extractSequences(this.versionString, this.versionSequences);
  }

  public BiosVersion(int major, int minor, int build, boolean isSnapshot) {
    final var sb = new StringBuilder(major).append(".").append(minor).append(".").append(build);
    if (isSnapshot) {
      snapshot = "-SNAPSHOT";
      sb.append(snapshot);
    } else {
      snapshot = "";
    }
    versionString = sb.toString();
    this.versionSequences = new int[MAX_DOTS_IN_VERSION + 1];
    versionSequences[0] = major;
    versionSequences[1] = minor;
    versionSequences[2] = build;
  }

  public boolean isSnapshotVersion() {
    return !snapshot.isEmpty();
  }

  public int getMajor() {
    return versionSequences[0];
  }

  public int getMinor() {
    return versionSequences[1];
  }

  public int getBuild() {
    return versionSequences[2];
  }

  public String getSnapshot() {
    return snapshot;
  }

  @Override
  public int compareTo(BiosVersion o) {
    if (o == null) {
      return 1;
    }
    int ret = 0;
    for (int i = 0; i <= MAX_DOTS_IN_VERSION; i++) {
      if (versionSequences[i] > o.versionSequences[i]) {
        ret = 1;
        break;
      } else if (versionSequences[i] < o.versionSequences[i]) {
        ret = -1;
        break;
      }
    }
    if (ret == 0) {
      if (snapshot.isEmpty()) {
        if (!o.snapshot.isEmpty()) {
          return 1;
        }
      } else if (o.snapshot.isEmpty()) {
        return -1;
      } else {
        return snapshot.compareTo(o.snapshot);
      }
    }
    return ret;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BiosVersion that = (BiosVersion) o;
    return Arrays.equals(versionSequences, that.versionSequences) && snapshot.equals(that.snapshot);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(versionSequences) + snapshot.hashCode();
  }

  @Override
  public String toString() {
    return versionString;
  }

  private static String extractSequences(String versionString, int[] outSequences) {
    int nextSequence = 0;
    int nextNum = 0;
    String snapshot = "";
    for (int i = 0; i < versionString.length(); i++) {
      final char c = versionString.charAt(i);
      if (c == '-') {
        snapshot = versionString.substring(i + 1);
        break;
      }
      if (c == '.') {
        if (nextSequence + 1 > MAX_DOTS_IN_VERSION) {
          // this should not happen. So throw a runtime if it does
          throw new RuntimeException("Invalid Version String " + versionString);
        }
        outSequences[nextSequence++] = nextNum;
        nextNum = 0;
      } else {
        int digit = c - '0';
        if (digit > 9) {
          // this should not happen. So throw a runtime if it does
          throw new RuntimeException("Invalid Version String " + versionString);
        }
        nextNum = (nextNum * 10) + digit;
      }
    }
    outSequences[nextSequence] = nextNum;
    return snapshot;
  }
}
