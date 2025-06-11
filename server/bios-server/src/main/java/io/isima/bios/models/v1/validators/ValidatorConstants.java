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
package io.isima.bios.models.v1.validators;

public class ValidatorConstants {

  public static final int MAX_NAME_LENGTH = 40;

  /* this patterns allows specific patterns even without allowedInternalSignals */
  public static final String NAME_PATTERN = "^\\p{Alnum}[\\p{Alnum}_]*$";
  public static final String INTERNAL_NAME_PATTERN = "^[\\p{Alnum}_]+$";

  public static final String CONTEXT_NAME_PATTERN = "^\\p{Alnum}[\\p{Alnum}_:]*$";
  public static final String INTERNAL_CONTEXT_NAME_PATTERN = "^[_\\p{Alnum}][\\p{Alnum}_:]*$";

  public static final String APP_NAME_PATTERN = "^[\\p{Alnum}_-]+$";

  public static final String URL_PATTERN = "[A-Za-z0-9-._~:/?#\\[\\]@!$&'()*+,;=]*";

  public static final int USERNAME_MAX_LENGTH = 256;

  public static final int PASSWORD_MAX_LENGTH = 8192;
  public static final String PASSWORD_PATTERN =
      "[a-zA-Z0-9!\"#$%&'()*+,-./:;<=>?@\\[\\\\\\]^_`{|}~]+";
}
