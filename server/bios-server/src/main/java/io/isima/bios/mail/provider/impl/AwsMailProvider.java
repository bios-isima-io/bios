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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AwsMailProvider implements MailProvider {
  private static Logger logger = LoggerFactory.getLogger(AwsMailProvider.class);

  private final Session session;

  private String configSet;
  private String smtpUserName;
  private String smtpPassword;

  public Session getSession() {
    return session;
  }

  public AwsMailProvider(
      final Properties props,
      final String configSet,
      final String userName,
      final String password) {
    this.session = Session.getInstance(props);
    this.configSet = configSet;
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
            msg.setContent(email.getTextHtml(), "text/html;charset=UTF-8");

            if (email.getBccAddresses() != null && email.getBccAddresses().length > 0) {
              msg.setRecipients(RecipientType.BCC, email.getBccAddresses());
            }

            if (email.getHeaders() != null && !email.getHeaders().isEmpty()) {
              for (Map.Entry<String, String> header : email.getHeaders().entrySet()) {
                msg.addHeader(header.getKey(), header.getValue());
              }
            }

            // Add a configuration set header.
            if (StringUtils.isNotEmpty(configSet)) {
              msg.addHeader("X-SES-CONFIGURATION-SET", configSet);
            }

            // Create a transport.
            Transport transport = session.getTransport();

            // Connect to SES mail service
            try {
              transport.connect(smtpUserName, smtpPassword);
            } catch (Exception ex) {
              logger.info("error in connecting {}", ex.toString());

              if (!transport.isConnected()) {
                transport.connect(smtpUserName, smtpPassword);
              }
            }

            msg.saveChanges();
            // Send the email.
            transport.sendMessage(msg, msg.getAllRecipients());
          } catch (MessagingException ex) {
            throw new RuntimeException("failed to send email", ex);
          }
        });
  }

  public void shutdown() {
    // Close and terminate the connection.
  }
}
