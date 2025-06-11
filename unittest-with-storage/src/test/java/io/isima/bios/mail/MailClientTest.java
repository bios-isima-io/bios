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
package io.isima.bios.mail;

import io.isima.bios.auth.v1.impl.AuthV1Impl;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.mail.impl.MailClientImpl;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class MailClientTest {

  private static String mailDisableStatus;

  @BeforeClass
  public static void setup() {
    Bios2TestModules.setProperty(TfosConfig.MAIL_DISABLED, "true");
    Bios2TestModules.startModules(false, MailClientTest.class, Map.of());
  }

  @AfterClass
  public static void cleanup() {
    Bios2TestModules.shutdown();
  }

  @Test
  public void testSignupEmail() {
    String subject = "signup";
    String to = "test@isima.io";

    MailClient mailClient = new MailClientImpl();

    long currentTime = System.currentTimeMillis();
    long expirationTime = currentTime + TfosConfig.signupExpirationTimeMillis();

    final String token = new AuthV1Impl().createSignupToken(currentTime, expirationTime, null, to);

    final String verifyUrlFormat = TfosConfig.signupVerifyBaseUrl();
    final String verifyUrl = String.format(verifyUrlFormat, token);

    final String mailContent = String.format(TfosConfig.signupMailContent(), to, verifyUrl);
    Email emailObj =
        Email.builder()
            .subject(subject)
            .textHtml(mailContent)
            .to(to)
            .from(TfosConfig.mailServiceFromAddress())
            .build();
    mailClient.sendMail(emailObj);
  }
}
