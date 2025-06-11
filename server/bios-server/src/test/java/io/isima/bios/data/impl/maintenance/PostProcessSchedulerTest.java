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
package io.isima.bios.data.impl.maintenance;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.math3.util.Pair;
import org.junit.Test;

public class PostProcessSchedulerTest {
  private static final long INTERVAL5 = 5000L;
  private static final long INTERVAL15 = 15000L;

  private static final String NAME1 = "visits.view.lastFileVisits";
  private static final String NAME2 = "visits.view.byRegion";

  @Test
  public void indexSpecAlignTestNoOverlap() {
    final long start1 = 1730398360000L;
    final var spec1 =
        new DigestSpecifier(
            NAME1,
            123L,
            start1,
            start1 + INTERVAL5,
            start1 - INTERVAL5,
            start1 + INTERVAL5,
            INTERVAL5);
    final long start2 = 1730398395000L;
    final var spec2 =
        new DigestSpecifier(
            NAME2, 123L, start2, start2 + INTERVAL15, start2, start2 + INTERVAL15, INTERVAL15);
    final var indexSpecs = new ArrayList<DigestSpecifier>();
    indexSpecs.addAll(List.of(spec1, spec2));
    final long origin = 1730398351000L;
    final long rollupPoint = 1730398429002L;
    PostProcessScheduler.alignSpecs(indexSpecs, origin, rollupPoint, 900000);

    verifySpecs(indexSpecs, origin, rollupPoint);
  }

  @Test
  public void indexSpecAlignTestDifferentProgress() {
    final long doneSince2 = 1730398395000L;
    final long doneSince1 = doneSince2 - 10000;
    final long origin = doneSince2 - 12000;
    final long rollupPoint = origin + 600150;

    final long doneUntil1 = doneSince1 + 205000L;
    final long doneUntil2 = doneSince2 + 495000L;

    final var spec1 =
        new DigestSpecifier(
            NAME1, 123L, doneUntil1 - INTERVAL5, doneUntil1, doneSince1, doneUntil1, INTERVAL5);
    final var spec2 =
        new DigestSpecifier(
            NAME2, 123L, doneUntil2 - INTERVAL15, doneUntil2, doneSince2, doneUntil2, INTERVAL15);
    final var indexSpecs = new ArrayList<DigestSpecifier>();
    indexSpecs.addAll(List.of(spec1, spec2));
    PostProcessScheduler.alignSpecs(indexSpecs, origin, rollupPoint, 900000);

    verifySpecs(indexSpecs, origin, rollupPoint);
  }

  @Test
  public void indexSpecAlignTestRetroactiveRollup() {
    final long rollupPoint = 1730399043000L;
    final long doneUntil1 = 1730399040000L;
    final long doneUntil2 = 1730399040000L;
    final long origin = 1730398383000L;

    final long doneSince1 = doneUntil1 - INTERVAL5;
    final long doneSince2 = doneUntil2 - INTERVAL15;

    final var spec1 =
        new DigestSpecifier(NAME1, 123L, doneSince1, doneUntil1, doneSince1, doneUntil1, INTERVAL5);
    final var spec2 =
        new DigestSpecifier(
            NAME2, 123L, doneSince2, doneUntil2, doneSince2, doneUntil2, INTERVAL15);
    final var indexSpecs = new ArrayList<DigestSpecifier>();
    indexSpecs.addAll(List.of(spec1, spec2));
    PostProcessScheduler.alignSpecs(indexSpecs, origin, rollupPoint, 900000);

    verifySpecs(indexSpecs, origin, rollupPoint);
  }

  private void verifySpecs(List<DigestSpecifier> indexSpecs, long origin, long rollupPoint) {
    final var ranges = new HashMap<String, Pair<Long, Long>>();
    for (var spec : indexSpecs) {
      assertThat(spec.getStartTime(), lessThan(spec.getEndTime()));
      assertThat(spec.getStartTime(), greaterThanOrEqualTo(spec.getDoneSince()));
      assertThat(spec.getDoneUntil(), lessThanOrEqualTo(spec.getDoneUntil()));
      final var currentRange = ranges.get(spec.getName());
      if (currentRange == null) {
        if (spec.getStartTime() != spec.getDoneSince()) {
          assertThat(spec.getEndTime(), equalTo(spec.getDoneUntil()));
        }
        ranges.put(spec.getName(), new Pair(spec.getDoneSince(), spec.getDoneUntil()));
      } else {
        final long nextDoneSince;
        final long nextDoneUntil;
        if (!currentRange.getSecond().equals(spec.getStartTime())) {
          assertThat(currentRange.getFirst(), equalTo(spec.getEndTime()));
          nextDoneSince = spec.getStartTime();
          nextDoneUntil = currentRange.getSecond();
        } else {
          nextDoneSince = currentRange.getFirst();
          nextDoneUntil = spec.getEndTime();
        }
        assertThat(nextDoneSince, equalTo(spec.getDoneSince()));
        assertThat(nextDoneUntil, equalTo(spec.getDoneUntil()));
        final var nextRange = new Pair(nextDoneSince, nextDoneUntil);
        ranges.put(spec.getName(), nextRange);
      }
    }
    final var finalRange1 = ranges.get(NAME1);
    assertThat(finalRange1.getFirst() - origin, lessThanOrEqualTo(INTERVAL15));
    assertThat(rollupPoint - finalRange1.getSecond(), lessThanOrEqualTo(INTERVAL15));
    final var finalRange2 = ranges.get(NAME2);
    assertThat(finalRange2.getFirst() - origin, lessThanOrEqualTo(INTERVAL15));
    assertThat(rollupPoint - finalRange2.getSecond(), lessThanOrEqualTo(INTERVAL15));
  }
}
