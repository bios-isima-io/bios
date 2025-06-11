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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.junit.MatcherAssert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.hamcrest.Matchers;
import org.junit.Test;

public class BatchedRollupCacheRequestTest {
  private static final ObjectMapper mapper = BiosObjectMapperProvider.get();

  @Test
  public void testMarshalling() throws IOException {
    List<RollupCacheRequest> reqs = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      RollupCacheRequest req = new RollupCacheRequest("t1" + i, "s1" + i, "f1" + i);
      req.setRollupTimestamp(System.currentTimeMillis());
      Event event1 = new EventJson();
      event1.set("test", "test");
      req.setEvents(Collections.singletonList(event1));
      req.setRollupTimestamp(System.currentTimeMillis());
      req.setRollupVersion(System.currentTimeMillis());
      assertThat(req.toString(), Matchers.containsString("t1" + i));
      assertThat(req.toString(), Matchers.containsString("s1" + i));
      assertThat(req.toString(), Matchers.containsString("f1" + i));
      reqs.add(req);
    }
    BatchedRollupCacheRequest batch = new BatchedRollupCacheRequest(reqs);
    final var json = batch.toString();
    assertThat(json, Matchers.containsString("t10"));
    assertThat(json, Matchers.containsString("t19"));
    assertThat(json, Matchers.containsString("s10"));
    assertThat(json, Matchers.containsString("s19"));
    assertThat(json, Matchers.containsString("f10"));
    assertThat(json, Matchers.containsString("f19"));

    BatchedRollupCacheRequest batch1 = new BatchedRollupCacheRequest();
    batch1.setRollups(reqs);
    final var json1 = batch.toString();
    assertThat(json1, Matchers.containsString("t10"));
    assertThat(json1, Matchers.containsString("f19"));

    final var req = mapper.readValue(json1, BatchedRollupCacheRequest.class);
    assertThat(req.rollups.get(0).getEvents().get(0).get("test"), is("test"));
  }
}
