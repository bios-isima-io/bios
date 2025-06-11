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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import javax.mail.Message.RecipientType;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.MimeMessage;

public class GoogleMailProvider implements MailProvider {
  private final Session session;
  private Transport transport;

  private String smtpUserName;
  private String smtpPassword;

  public Session getSession() {
    return session;
  }

  public GoogleMailProvider(final Properties props, final String userName, final String password) {
    this.session = Session.getInstance(props);
    this.smtpUserName = userName;
    this.smtpPassword = password;
  }

  @Override
  public CompletableFuture<Void> sendMail(Email email) {
    return CompletableFuture.runAsync(
        () -> {
          try {
            // Create a message with the specified information.
            MimeMessage msg = new MimeMessage(session);

            msg.setFrom(email.getFromAddress());
            msg.setRecipients(RecipientType.TO, email.getToAddresses());
            msg.setSubject(email.getSubject());
            msg.setContent(email.getTextHtml(), "text/html");

            if (email.getHeaders() != null && !email.getHeaders().isEmpty()) {
              for (Map.Entry<String, String> header : email.getHeaders().entrySet()) {
                msg.addHeader(header.getKey(), header.getValue());
              }
            }

            if (transport == null || !transport.isConnected()) {
              // Create a transport.
              transport = session.getTransport();
              // Connect to Google mail service
              transport.connect(smtpUserName, smtpPassword);
            }

            // Send the email.
            msg.saveChanges();
            transport.sendMessage(msg, msg.getAllRecipients());
          } catch (MessagingException ex) {
            throw new RuntimeException("failed to send email", ex);
          }
        });
  }

  public void shutdown() {
    // Close and terminate the connection.
    try {
      if (transport != null) {
        transport.close();
      }
    } catch (MessagingException ex) {
      throw new RuntimeException("failed to close connection to provider", ex);
    }
  }
}
