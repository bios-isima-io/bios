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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class EmailTest {

  @Test
  public void test() {
    String htmlContent = "<p>test</p>";
    String toEmail = "xyz@test.com";
    String subject = "test mail";
    String fromName = "abc";
    String fromEmail = "abc@test.com";

    Email emailObj =
        new Email.Builder()
            .textHtml(htmlContent)
            .subject(subject)
            .to(toEmail)
            .from(fromEmail)
            .fromName(fromName)
            .build();

    assertNotNull(emailObj);
    assertEquals(htmlContent, emailObj.getTextHtml());
    assertEquals(subject, emailObj.getSubject());

    assertNotNull(emailObj.getFromAddress());
    assertEquals(fromEmail, emailObj.getFromAddress().getAddress());
    assertEquals(fromName, emailObj.getFromAddress().getPersonal());

    assertNotNull(emailObj.getToAddresses());
    assertEquals(1L, emailObj.getToAddresses().length);
    assertNotNull(emailObj.getToAddresses()[0]);
    assertEquals(toEmail, emailObj.getToAddresses()[0].getAddress());
  }
}
