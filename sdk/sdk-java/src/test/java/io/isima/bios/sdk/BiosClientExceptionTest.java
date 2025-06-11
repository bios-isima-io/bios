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
package io.isima.bios.sdk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.isima.bios.sdk.errors.BiosClientError;
import io.isima.bios.sdk.exceptions.BiosClientException;
import org.junit.Test;

public class BiosClientExceptionTest {

  @Test
  public void testConstructorWithMessage() {
    BiosClientException ex = new BiosClientException("Test message");
    assertEquals(BiosClientError.GENERIC_CLIENT_ERROR, ex.getCode());
    assertEquals("Test message", ex.getMessage());

    String expectedException =
        getExpectedExceptionString(
            "io.isima.bios.sdk.exceptions.BiosClientException:"
                + " %s; errorCode=%s, message='Test message'",
            BiosClientError.GENERIC_CLIENT_ERROR);

    assertEquals(expectedException, ex.toString());
  }

  @Test
  public void testConstructorWithCodeAndMessage() {
    BiosClientException ex = new BiosClientException(BiosClientError.NO_SUCH_STREAM, "overriding");
    assertEquals(BiosClientError.NO_SUCH_STREAM, ex.getCode());
    assertEquals("overriding", ex.getMessage());
    String expectedException =
        getExpectedExceptionString(
            "io.isima.bios.sdk.exceptions.BiosClientException:"
                + " %s; errorCode=%s, message='overriding'",
            BiosClientError.NO_SUCH_STREAM);
    assertEquals(expectedException, ex.toString());
  }

  @Test
  public void testConstructorWithCodeMessageAndEndpoint() {
    BiosClientException ex =
        new BiosClientException(BiosClientError.BAD_INPUT, "Corrupted", "129.18.0.1:443");
    assertEquals(BiosClientError.BAD_INPUT, ex.getCode());
    assertEquals("Corrupted", ex.getMessage());
    assertEquals("129.18.0.1:443", ex.getEndpoint());
    String expectedException =
        getExpectedExceptionString(
            "io.isima.bios.sdk.exceptions.BiosClientException:"
                + " %s; errorCode=%s, endpoint=129.18.0.1:443, message='Corrupted'",
            BiosClientError.BAD_INPUT);
    assertEquals(expectedException, ex.toString());
  }

  @Test
  public void testConstructorWithCause() {
    BiosClientException ex = new BiosClientException("Abcdefg", new RuntimeException());
    assertEquals(BiosClientError.GENERIC_CLIENT_ERROR, ex.getCode());
    assertTrue(ex.getCause() instanceof RuntimeException);
    assertEquals("Abcdefg", ex.getMessage());
    String expectedException =
        getExpectedExceptionString(
            "io.isima.bios.sdk.exceptions.BiosClientException:"
                + " %s; errorCode=%s, message='Abcdefg', caused by: java.lang.RuntimeException",
            BiosClientError.GENERIC_CLIENT_ERROR);
    assertEquals(expectedException, ex.toString());
  }

  private String getExpectedExceptionString(String formatString, BiosClientError tfosClientError) {
    return String.format(formatString, tfosClientError.name(), tfosClientError.getErrorCode());
  }
}
