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
package io.isima.bios.mail.impl;

import io.isima.bios.common.TfosConfig;
import io.isima.bios.mail.Email;
import io.isima.bios.mail.MailClient;
import io.isima.bios.mail.provider.MailProvider;
import io.isima.bios.mail.provider.impl.AwsMailProvider;
import io.isima.bios.mail.provider.impl.GcpMailProvider;
import io.isima.bios.mail.provider.impl.GoogleMailProvider;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.StringUtils;

public class MailClientImpl implements MailClient {

  private final MailProvider mailProvider;
  private final boolean isDisabled;

  public MailClientImpl() {
    isDisabled = TfosConfig.mailDisabled();
    if (isDisabled) {
      mailProvider = null;
    } else {
      String provider = TfosConfig.mailProvider();
      String host = TfosConfig.mailServiceHost();

      Properties props = new Properties();
      props.put("mail.transport.protocol", "smtp");

      if (TfosConfig.mailAuthEnabled()) {
        props.put("mail.smtp.auth", "true");
      }

      // props.put("mail.debug", "true");
      props.put("mail.smtp.host", host);
      props.put("mail.smtp.ssl.trust", host);
      props.put("mail.smtp.ssl.checkserveridentity", "true");

      final int port;
      if (TfosConfig.mailSecureConnectionDisabled()) {
        if (!TfosConfig.isTestMode()) {
          throw new RuntimeException(
              String.format(
                  "%s can be set only in test mode", TfosConfig.MAIL_SECURE_CONNECTION_DISABLED));
        }
        port = TfosConfig.mailSmtpNonSecurePort();
      } else if (TfosConfig.mailTlsEnabled()) {
        port = TfosConfig.mailSmtpTlsPort();
        props.put("mail.smtp.starttls.required", "true");
        props.put("mail.smtp.starttls.enable", "true");
      } else {
        port = TfosConfig.mailSmtpSslPort();
        props.put("mail.smtp.ssl.enable", "true");
      }
      props.put("mail.smtp.port", port);

      if (provider.equalsIgnoreCase("aws")) {
        String userName = TfosConfig.mailServiceUserName();
        String password = TfosConfig.mailServicePassword();
        if (StringUtils.isBlank(userName) || StringUtils.isBlank(password)) {
          throw new RuntimeException("mail service credentials not set");
        }
        mailProvider = new AwsMailProvider(props, "", userName, password);
      } else if (provider.equalsIgnoreCase("gmail")) {
        String userName = TfosConfig.mailServiceUserName();
        String password = TfosConfig.mailServicePassword();
        if (StringUtils.isBlank(userName) || StringUtils.isBlank(password)) {
          throw new RuntimeException("mail service credentials not set");
        }
        mailProvider = new GoogleMailProvider(props, userName, password);
      } else {
        mailProvider = new GcpMailProvider(props);
      }
    }
  }

  @Override
  public boolean validate(Email email) {
    return true;
  }

  @Override
  public void sendMail(Email email) {
    if (isDisabled) {
      return;
    }
    try {
      mailProvider.sendMail(email).get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("sending mail interrupted", e);
    } catch (ExecutionException e) {
      throw new RuntimeException("failed to send email", e);
    }
  }

  @Override
  public CompletableFuture<Void> sendMailAsync(Email email) {
    if (isDisabled) {
      return CompletableFuture.completedFuture(null);
    }
    return mailProvider.sendMail(email);
  }

  public void shutdown() {
    if (isDisabled) {
      return;
    }
    mailProvider.shutdown();
  }
}
