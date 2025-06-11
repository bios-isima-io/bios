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
package io.isima.bios.mail.provider.impl;

import io.isima.bios.mail.Email;
import io.isima.bios.mail.provider.MailProvider;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import javax.mail.Session;

public class GcpMailProvider implements MailProvider {
  private final Session session;

  public Session getSession() {
    return session;
  }

  public GcpMailProvider(final Properties props) {
    this.session = Session.getDefaultInstance(props);
  }

  @Override
  public CompletableFuture<Void> sendMail(Email email) {
    return null;
  }

  public void shutdown() {}
}
