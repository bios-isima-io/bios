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
package io.isima.bios.errors.exception;

import io.isima.bios.errors.BiosError;
import io.isima.bios.errors.GenericError;
import io.isima.bios.models.UserContext;
import java.util.Objects;
import javax.ws.rs.core.Response.Status;
import lombok.Getter;
import lombok.Setter;

/**
 * An exception thrown when an application error occurred.
 *
 * <p>When a web service receives an exception, treat it as a Internal Server Error.
 */
public class TfosException extends Exception implements BiosError {

  private static final long serialVersionUID = 2001848243960830720L;

  protected final BiosError info;

  protected String mymessage;
  // Internal error message used for troubleshooting, not returned to the user
  @Setter @Getter protected String internalMessage;

  protected UserContext userContext;
  protected Object context;

  public TfosException(String message) {
    this.info = GenericError.APPLICATION_ERROR;
    mymessage = message;
  }

  public TfosException(String message, Throwable t) {
    super(t);
    this.info = GenericError.APPLICATION_ERROR;
    mymessage = message;
  }

  public TfosException(BiosError info) {
    Objects.requireNonNull(info, "'info' must not be null");
    mymessage = info.getErrorMessage();
    this.info = info;
  }

  public TfosException(TfosException cause) {
    super(cause.getMessage(), cause);
    this.info = cause.info;
  }

  public TfosException(String message, TfosException cause) {
    super(message, cause);
    this.info = cause.info;
  }

  public TfosException(BiosError info, String additionalMessage) {
    Objects.requireNonNull(info, "'info' must not be null");
    mymessage = info.getErrorMessage() + ": " + additionalMessage;
    this.info = info;
  }

  public TfosException(BiosError info, Throwable t) {
    super(t);
    Objects.requireNonNull(info, "'info' must not be null");
    mymessage = info.getErrorMessage();
    this.info = info;
  }

  public TfosException(BiosError info, String additionalMessage, Throwable t) {
    super(t);
    Objects.requireNonNull(info, "'info' must not be null");
    mymessage = info.getErrorMessage() + ": " + additionalMessage;
    this.info = info;
  }

  public UserContext getUserContext() {
    return userContext;
  }

  public TfosException setUserContext(UserContext userContext) {
    this.userContext = userContext;
    return this;
  }

  public Object getContext() {
    return context;
  }

  public TfosException setContext(Object context) {
    this.context = context;
    return this;
  }

  @Override
  public String getErrorCode() {
    return info.getErrorCode();
  }

  public String getErrorName() {
    if (info instanceof Exception) {
      return ((TfosException) info).getErrorName();
    }
    return info.toString();
  }

  @Override
  public Status getStatus() {
    return info.getStatus();
  }

  @Override
  public String getMessage() {
    return mymessage;
  }

  @Override
  public String getErrorMessage() {
    return getMessage();
  }

  public void appendMessage(final String additional) {
    mymessage += "; " + additional;
  }

  public void replaceMessage(String newMessage) {
    mymessage = newMessage;
  }

  @Override
  public String toString() {
    final var sb = new StringBuilder(super.toString());
    if (internalMessage != null) {
      sb.append(" (").append(internalMessage).append(" )");
    }
    return sb.toString();
  }
}
