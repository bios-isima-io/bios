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
package io.isima.bios.load.utils;

public class Enums {

  // device type enum
  enum DeviceType {
    Android, iOS, Laptop, Desktop, Embedded
  };

  // Network Type Enum
  enum NetworkType {
    Cellular, WiFi
  };

  enum BrowserType {
    Chrome, Firefox, Safari, Opera, InternetExplorer
  }

  enum Gender {
    Male, Female
  }

  enum PaymentType {
   Cash, Card, Wallet, NetBanking, Cheque, EMI, Loan
  }
}
