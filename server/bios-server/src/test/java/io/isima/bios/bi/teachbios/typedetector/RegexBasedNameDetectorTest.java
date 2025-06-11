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
package io.isima.bios.bi.teachbios.typedetector;

import io.isima.bios.bi.teachbios.namedetector.NameMatchTieBreaker;
import io.isima.bios.bi.teachbios.namedetector.PriorityBasedTieBreaker;
import io.isima.bios.bi.teachbios.namedetector.RegexBasedNameDetector;
import io.isima.bios.errors.exception.FileReadException;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RegexBasedNameDetectorTest {

  private RegexBasedNameDetector regexBasedNameDetector;
  private NameMatchTieBreaker nameMatchTieBreaker;
  private final String domain = "ecommerce";

  @Before
  public void setUp() throws FileReadException {
    nameMatchTieBreaker = new PriorityBasedTieBreaker();
    regexBasedNameDetector = new RegexBasedNameDetector(nameMatchTieBreaker);
  }

  @Test
  public void testCreditCardMatch() {
    List<String> inputValues =
        Arrays.asList(
            "4386-2900-2004-2718",
            "4386-2804-2124-2718",
            "378282246310005",
            "4012888888881881",
            "341111111111111",
            "3422222222222222");
    String inferredColumnName = regexBasedNameDetector.inferColumnName(inputValues, domain);
    Assert.assertEquals("creditcard", inferredColumnName);
  }

  @Test
  public void testEmailMatch() {
    List<String> inputValues =
        Arrays.asList("vimal@isima.io", "mehul@isima.io", "prasanta@isima.io");
    String inferredColumnName = regexBasedNameDetector.inferColumnName(inputValues, domain);
    Assert.assertEquals("email", inferredColumnName);
  }

  @Test
  public void testNameMatch() {
    List<String> inputValues = Arrays.asList("John Wick", "John Doe", "Vimal Sharma");
    String inferredColumnName = regexBasedNameDetector.inferColumnName(inputValues, domain);
    Assert.assertEquals("name", inferredColumnName);
  }

  @Test
  public void testPriceMatch() {
    List<String> inputValues = Arrays.asList("12", "100", "43.1", "34.234", "43.12");
    String inferredColumnName = regexBasedNameDetector.inferColumnName(inputValues, domain);
    Assert.assertEquals("price", inferredColumnName);
  }

  @Test
  public void testZipCodeMatch() {
    List<String> inputValues = Arrays.asList("36104", "99801", "560031", "784012");
    String inferredColumnName = regexBasedNameDetector.inferColumnName(inputValues, domain);
    Assert.assertEquals("zipcode", inferredColumnName);
  }

  @Test
  public void testPhoneNumberMatch() {
    List<String> inputValues =
        Arrays.asList(
            "7406557823", "8576980003", "(541) 754-3010", "+917406557823", "+1 717-827-7017");
    String inferredColumnName = regexBasedNameDetector.inferColumnName(inputValues, domain);
    Assert.assertEquals("phonenumber", inferredColumnName);
  }

  @Test
  public void testIpAddressMatch() {
    List<String> inputValues =
        Arrays.asList("127.0.0.1", "172.43.56.123", "209.43.12.54", "213.78.90.123");
    String inferredColumnName = regexBasedNameDetector.inferColumnName(inputValues, domain);
    Assert.assertEquals("ipaddress", inferredColumnName);
  }

  @Test
  public void testSSNMatch() {
    List<String> inputValues =
        Arrays.asList("243-76-6991", "595-94-2802", "289-82-1991", "456-78-9101");
    String inferredColumnName = regexBasedNameDetector.inferColumnName(inputValues, domain);
    Assert.assertEquals("ssn", inferredColumnName);
  }

  @Test
  public void testDateMatch() {
    List<String> inputValues =
        Arrays.asList(
            "21-06-1991", "22/07/2019", "27-08-2019", "01-01-2001", "2018-09-12", "2018/09/12");
    String inferredColumnName = regexBasedNameDetector.inferColumnName(inputValues, domain);
    Assert.assertEquals("date", inferredColumnName);
  }

  @Test
  public void testUrlMatch() {
    List<String> inputValues =
        Arrays.asList(
            "www.google.com",
            "https://www.youtube.com/watch?v=fV4x7OCqFLA",
            "https://www.baeldung.com/java-find-map-max",
            "https://maven.apache.org/plugins/maven-checkstyle-plugin/check-mojo.html");
    String inferredColumnName = regexBasedNameDetector.inferColumnName(inputValues, domain);
    Assert.assertEquals("url", inferredColumnName);
  }

  @Test
  public void testTimestampMatch() {
    List<String> inputValues =
        Arrays.asList(
            "2017-02-14 19:30",
            "2017-02-14 19:30:45",
            "2017/02/14 19:30:32",
            "14-02-2017 19:30:32",
            "14/02/2017 19:30:32");
    String inferredColumnName = regexBasedNameDetector.inferColumnName(inputValues, domain);
    Assert.assertEquals("timestamp", inferredColumnName);
  }

  @Test
  public void testAlternativeDateFormat() {
    List<String> dateValues =
        Arrays.asList("7/27/2012", "9/8/2014", "8/27/2012", "9/3/2012", "8/27/2010");
    String inferredColumnName = regexBasedNameDetector.inferColumnName(dateValues, domain);
    Assert.assertEquals("date", inferredColumnName);
  }

  @Test
  public void testNoKeywordIsMatchedIfNoMajority() {
    List<String> inputValues =
        Arrays.asList("4386-2900-2004-2718", "3422222222222222", "CreditCard");
    String inferredColumnName = regexBasedNameDetector.inferColumnName(inputValues, domain);
    Assert.assertEquals("NA", inferredColumnName);
  }

  @Test
  public void testAmexCreditCardHyphenSeparated() {
    List<String> inputValues =
        Arrays.asList("3742-4545-5400-1261", "3742-4545-5230-1261", "3742-4545-5400-1212");
    String inferredColumnName = regexBasedNameDetector.inferColumnName(inputValues, domain);
    Assert.assertEquals("creditcard", inferredColumnName);
  }
}
