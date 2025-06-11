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

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;

public class Email {
  private final InternetAddress fromAddress;
  private final InternetAddress[] toAddresses;
  private final InternetAddress[] bccAddresses;
  private final String subject;
  private final String text;
  private final String textHtml;
  private final Map<String, String> headers;

  private Email(Builder builder) throws IllegalArgumentException {
    try {
      fromAddress = new InternetAddress(builder.from, builder.fromName);
    } catch (UnsupportedEncodingException ex) {
      throw new IllegalArgumentException("invalid from address", ex);
    }

    subject = builder.subject;
    text = builder.text;
    textHtml = builder.textHtml;
    headers = builder.headers;

    toAddresses = new InternetAddress[builder.to.size()];
    int index = 0;
    for (String to : builder.to) {
      try {
        toAddresses[index++] = new InternetAddress(to);
      } catch (AddressException ex) {
        throw new IllegalArgumentException("invalid to address", ex);
      }
    }

    if (builder.bcc == null || builder.bcc.isEmpty()) {
      bccAddresses = null;
    } else {
      bccAddresses = new InternetAddress[builder.bcc.size()];
      index = 0;
      for (String bcc : builder.bcc) {
        try {
          bccAddresses[index++] = new InternetAddress(bcc);
        } catch (AddressException ex) {
          throw new IllegalArgumentException("invalid bcc address", ex);
        }
      }
    }
  }

  public InternetAddress getFromAddress() {
    return fromAddress;
  }

  public InternetAddress[] getToAddresses() {
    return toAddresses;
  }

  public InternetAddress[] getBccAddresses() {
    return bccAddresses;
  }

  public String getSubject() {
    return subject;
  }

  public String getText() {
    return text;
  }

  public String getTextHtml() {
    return textHtml;
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String from;
    private String fromName;
    private List<String> to;
    private List<String> bcc = null;

    private String subject;
    private String text;
    private String textHtml;
    private Map<String, String> headers;

    public Builder from(String from) {
      this.from = from;
      return this;
    }

    public Builder fromName(String fromName) {
      this.fromName = fromName;
      return this;
    }

    public Builder to(String toMail) {
      if (to == null) {
        to = new ArrayList<>();
      }
      to.add(toMail);
      return this;
    }

    public Builder to(List<String> toList) {
      if (to == null) {
        to = toList;
      } else {
        to.addAll(toList);
      }
      return this;
    }

    public Builder bcc(String bccMail) {
      if (bcc == null) {
        bcc = new ArrayList<>();
      }
      bcc.add(bccMail);
      return this;
    }

    public Builder bcc(List<String> bccList) {
      if (bccList == null || bccList.isEmpty()) {
        return this;
      }
      if (bcc == null) {
        bcc = bccList;
      } else {
        bcc.addAll(bccList);
      }
      return this;
    }

    public Builder subject(String subject) {
      this.subject = subject;
      return this;
    }

    public Builder text(String text) {
      this.text = text;
      return this;
    }

    public Builder textHtml(String textHtml) {
      this.textHtml = textHtml;
      return this;
    }

    public Builder headers(Map<String, String> headers) {
      if (this.headers == null) {
        this.headers = headers;
      } else {
        this.headers.putAll(headers);
      }
      return this;
    }

    public Builder headers(String name, String value) {
      if (headers == null) {
        headers = new HashMap<>();
      }
      headers.put(name, value);
      return this;
    }

    public Email build() {
      Email email = new Email(this);
      return email;
    }
  }
}
