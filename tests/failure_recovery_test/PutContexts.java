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
import io.isima.bios.sdk.Bios;
import io.isima.bios.sdk.Session;
import java.util.Arrays;
import java.util.List;

class PutContexts {

  public static void main(String args[]) {
    if (args.length < 2) {
      System.out.println("Usage: PutContexts <count> <value_postfix>");
      System.exit(1);
    }
    final int count = Integer.parseInt(args[0]);
    final String postfix = args[1];
    Session.Starter starter = Bios.newSession("localhost").port(443)
        .user("admin@failure_recovery")
        .password("admin");
    try (final Session session = starter.connect()) {
      for (int i = 0; i < count; ++i) {
        final String src = i + "00," + i + postfix;
        final var statement = Bios.isql().upsert()
            .intoContext("my_context")
            .csvBulk(List.of(src))
            .build();
        session.execute(statement);
        Thread.sleep(100);
      }
      // System.out.println("done");
      System.exit(0);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
}
