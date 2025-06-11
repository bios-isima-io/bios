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
package io.isima.bios.data.service;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.javafaker.Address;
import com.github.javafaker.Faker;
import com.mifmif.common.regex.Generex;

import io.isima.bios.exception.ConfigException;
import io.isima.bios.exception.ContextKeyNotFoundException;
import io.isima.bios.exception.InvalidRegexException;
import io.isima.bios.exception.InvalidValueTypeException;
import io.isima.bios.load.model.AttributeDesc;
import io.isima.bios.load.model.MappedSignalValue;
import io.isima.bios.load.model.SignalMap;
import io.isima.bios.load.model.StreamConfig;
import io.isima.bios.load.model.StreamType;
import io.isima.bios.load.utils.ApplicationUtils;
import io.isima.bios.load.utils.RegexConstant;

public class DataStore {
  private final Logger logger = LoggerFactory.getLogger(DataStore.class);
  private final Map<String, List<String>> contextKeyMap = new HashMap<>();
  private final Map<String, List<String>> contextNameKeyMap = new ConcurrentHashMap<>();
  private final Faker fake = new Faker(new Locale("en-AU"));
  private final String pattern = "yyyy-MM-dd";
  private final String patternDateTime = "yyyy-MM-dd HH:mm";
  private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
  private final SimpleDateFormat simpleDateTimeFormat = new SimpleDateFormat(patternDateTime);
  private Config config;

  public DataStore(final Config config) {
    this.config = config;
  }

  /**
   * Method to generate signal/context event
   *
   * @param stream
   * @return String (Event)
   */
  public String generateEvent(final String stream) {
    final StreamConfig streamConfig = config.getStream(stream);
    if (null == streamConfig) {
      throw new ConfigException(String.format("Stream config not found for %s stream", stream));
    }
    if (streamConfig.getType().equals(StreamType.CONTEXT)) {
      return generateContextEvent(streamConfig);
    }
    return generateSignalEvent(streamConfig);
  }


  /**
   * Method to generate context event
   *
   * @param config
   * @return String (Context Event)
   */
  private String generateContextEvent(final StreamConfig config) {
    final StringJoiner joiner = new StringJoiner(",");
    final Address address = fake.address();
    final List<AttributeDesc> attributes = config.getAttributes();
    final String key = String.valueOf(generateData(config.getAttributes().get(0), fake.address(), config));
    if (null == contextKeyMap.get(attributes.get(0).getName())
            || contextKeyMap.get(attributes.get(0).getName()).isEmpty()) {
      List<String> keys = new ArrayList<>();
      contextKeyMap.put(attributes.get(0).getName(), keys);
      List<String> contextKeys = new LinkedList<>();
      contextNameKeyMap.put(config.getName(), contextKeys);
    }
    if (null == contextNameKeyMap.get(config.getName())
            || contextNameKeyMap.get(config.getName()).isEmpty()) {
      List<String> contextKeys = new LinkedList<>();
      contextNameKeyMap.put(config.getName(), contextKeys);
    }
    contextKeyMap.get(attributes.get(0).getName()).add(key);
    contextNameKeyMap.get(config.getName()).add(key);

    joiner.add(key);
    for (int i = 1; i < attributes.size(); i++) {
      joiner.add(String.valueOf(generateData(attributes.get(i), address, config)));
    }
    return joiner.toString();
  }

  public String getContextkey(final String attrName, final String context) {
    if (!contextKeyMap.containsKey(attrName) && !contextNameKeyMap.containsKey(context)) {
      throw new ContextKeyNotFoundException(
              String.format("No keys present for context %s, key attr %s ", context, attrName));
    }
    if (contextKeyMap.containsKey(attrName)) {
      return contextKeyMap.get(attrName)
              .get(RandomUtils.nextInt(0, contextKeyMap.get(attrName).size()));
    }
    return contextNameKeyMap.get(context)
            .get(RandomUtils.nextInt(0, contextNameKeyMap.get(context).size()));
  }

  /**
   * Method to generate signal event
   *
   * @param config
   * @return String (signal event)
   */
  private String generateSignalEvent(final StreamConfig config) {
    final StringJoiner joiner = new StringJoiner(",");
    final Address address = fake.address();
    
    // start date time stamp
    long startDateTimeStamp=System.currentTimeMillis();
    for (AttributeDesc attr : config.getAttributes()) {
    	
		// for telco only
    	if(attr.getName().trim().equalsIgnoreCase("eventDate") || attr.getName().trim().equalsIgnoreCase("startDate")) {
    		int min = attr.getAttrValType().getMinSize() == 0 ? 0: attr.getAttrValType().getMinSize() ;
    	    int max = attr.getAttrValType().getMaxSize() == 0 ? 1: attr.getAttrValType().getMaxSize() ;
    		Date startDate = fake.date().between(new DateTime().minusDays(max).toDate(), new DateTime().minusDays(min).toDate());
			joiner.add(simpleDateFormat.format(startDate));
			startDateTimeStamp=startDate.getTime();
			continue;
		}
		if(attr.getName().equalsIgnoreCase("eventTime") || attr.getName().equalsIgnoreCase("startTime") ) {
			joiner.add(simpleDateTimeFormat.format(new Date(startDateTimeStamp)));
			continue;
		}
		if(attr.getName().equalsIgnoreCase("endDate")) {
			String endDate = simpleDateFormat.format(new DateTime(startDateTimeStamp).plusMinutes(RandomUtils.nextInt(30, 240)).toDate());
			joiner.add(String.valueOf(endDate));
			continue;
		}
		if(attr.getName().equalsIgnoreCase("endTime")) {
			long endTimeStamp = new DateTime(startDateTimeStamp).plusMinutes(RandomUtils.nextInt(30, 240)).getMillis();
			joiner.add(simpleDateTimeFormat.format(new Date(endTimeStamp)));
			continue;
		}
		if(attr.getName().equalsIgnoreCase("eventTimeBucket5m")) {
			long millis5mBucket = 60*5*1000L;
			long r = startDateTimeStamp % millis5mBucket;
			Long eventTimeBucket5m = startDateTimeStamp - r;
			joiner.add(simpleDateTimeFormat.format(new Date(eventTimeBucket5m)));
			continue;
		}
		// telco custom logic ends here
		
      String attributeValue = null;
      if (config.isAttrMapped(attr.getName())) {
        final SignalMap mappedSignal = config.getMappedSignal(attr.getName());
        attributeValue = MappedSignalValue.getKey(mappedSignal.getDestSignalName(),
                mappedSignal.getDestinationKeyAttributes());
      } else if (null != contextKeyMap && contextKeyMap.containsKey(attr.getName())) {
        attributeValue = getContextkey(attr.getName(), config.getName());
      }
      if (null == attributeValue) {
        attributeValue = String.valueOf(generateData(attr, address, config));
      }
      // TODO experimenntal feature
      // add this value to key list if this attr is marked in stream config
      if (config.attrMarkedAsKey(attr.getName())) {
        MappedSignalValue.addKey(config.getName(), attr.getName(), attributeValue);
      }
      joiner.add(attributeValue);
    }
    return joiner.toString();
  }

  public String genAttrValue(final AttributeDesc attr, final StreamConfig config) {
    String attributeValue = null;
    // fetch value from mapped list if this attribute is mapped
    if (config.isAttrMapped(attr.getName())) {
      final SignalMap mappedSignal = config.getMappedSignal(attr.getName());
      attributeValue = MappedSignalValue.getKey(mappedSignal.getDestSignalName(),
              mappedSignal.getDestinationKeyAttributes());
    }
    if (null == attributeValue) {
      attributeValue = String.valueOf(generateData(attr, fake.address(), config));
    }
    //TODO experimenntal feature
    // add this value to key list if this attr is marked in stream config
    if (null != config.getKeyAttributes() && config.getKeyAttributes().contains(attr.getName())) {
      MappedSignalValue.addKey(config.getName(), attr.getName(), attributeValue);
    }
    return attributeValue;
  }


  private Object generateData(final AttributeDesc attr, final Address address,
                              final StreamConfig config) {
    if (null == attr.getAttrValType() || null == attr.getAttrValType().getValType()) {
      throw new InvalidValueTypeException(
              String.format("Invalid value type for attribute %s, stream config %s", attr.getName(),
                      config.getName()));
    }
    int min = attr.getAttrValType().getMinSize();
    int max = attr.getAttrValType().getMaxSize();
    switch (attr.getAttrValType().getValType()) {
      case NUMERIC_ID:
        return RandomUtils.nextLong(min, max);
      case STRING_ID:
        return RandomStringUtils.randomAlphabetic(min, max);
      case ADDRESS:
        return address.fullAddress().replaceAll(",", " ");
      case AGE:
        return RandomUtils.nextInt(min, max);
      case BROWSER:
        return ApplicationUtils.getBrowserType();
      case CARD_BRAND:
        return fake.finance().bic();
      case CARD_CVV:
        return getRegexString(RegexConstant.CVV, attr);
      case CARD_EXPIRY:
        return fake.business().creditCardExpiry();
      case CARD_TYPE:
        return fake.business().creditCardType();
      case CITY:
        return address.city().replaceAll(",", " ");
      case COMPANY_BUZZ_WORD:
        return fake.company().buzzword().replaceAll(",", " ");
      case COMPANY_NAME:
        return fake.company().name().replaceAll(",", " ");
      case COUNTRY:
        return address.country().replaceAll(",", " ");
      case CURRENCY_CODE:
        return "USD";
      case DEPARTMENT:
        return fake.commerce().department().replaceAll(",", " ");
      case DESCRIPTION:
        return fake.lorem().sentence().replaceAll(",", " ");
      case DEVICE_TYPE:
        return ApplicationUtils.getDeviceType();
      case EMAIL:
        return fake.internet().emailAddress();
      case GENDER:
        return ApplicationUtils.getGender();
      case INDUSTRY:
        return fake.company().industry().replaceAll(",", " ");
      case NAME:
        return fake.name().fullName();
      case NETWORK_TYPE:
        return ApplicationUtils.getNetworkType();
      case PHONE:
        return fake.phoneNumber().cellPhone().replaceAll(" ", "");
      case PINCODE:
        return getRegexString(RegexConstant.PINCODE, attr);
      case PRICE:
        return RandomUtils.nextInt(min, max);
      case PRODUCT_NAME:
        return fake.commerce().productName().replaceAll(",", " ");
      case PROFESSION:
        return fake.company().profession();
      case PROMO_CODE:
        return fake.commerce().promotionCode();
      case RANDOM_NUMBER:
        return RandomUtils.nextInt(min, max);
      case RANDOM_STRING:
        return RandomStringUtils.randomAlphabetic(min, max);
      case STATE:
        return address.state();
      case URL:
        return fake.internet().url();
      case VERSION:
        return fake.regexify(RegexConstant.VERSION);
      case BLOB:
        return null;
      case BOOLEAN:
        return RandomUtils.nextBoolean();
      case DATE:
        return simpleDateFormat
                .format(new Date());
      case DATE_BETWEEN:
          return simpleDateFormat
                  .format(fake.date().between(new DateTime().minusDays(max).toDate(), new DateTime().minusDays(min).toDate()));
      case ENUM:
        return attr.getEnum().get(RandomUtils.nextInt(0, attr.getEnum().size()));
      case INET:
        return fake.internet().ipV4Address();
      case TIMESTAMP:
        return fake.date().between(new DateTime().minusDays(5).toDate(), new Date()).getTime();
      case UUID:
        return UUID.randomUUID();
      case PANCARD:
        return getRegexString(RegexConstant.PANCARD, attr);
      case CARD_NUMBER:
        return getRegexString(RegexConstant.CARD_NUMBER, attr);
      case PAYMENT_TYPE:
        return ApplicationUtils.getPaymentType();
      case REGEX:
        return getRegexString(attr.getPattern(), attr);
      default:
        break;
    }
    return RandomUtils.nextInt(0, 9999);
  }


  public void removeKey(String key, String value) {
    if (contextNameKeyMap.containsKey(key)) {
      contextNameKeyMap.get(key).remove(value);
    }
  }

  private String getRegexString(final String regex, final AttributeDesc attr) {
    if (null == regex || regex.trim().isEmpty()) {
      throw new InvalidRegexException(
              String.format("Regex pattern must not be null or empty, Attribute %s", attr.getName()));
    }
    try {
      Pattern.compile(regex);
    } catch (PatternSyntaxException pse) {
      throw new InvalidRegexException(
              String.format("Regex is not valid, Attribute %s", attr.getName()), pse);
    }
    Generex generex = new Generex(regex);
    generex.setSeed(RandomUtils.nextLong());
    return generex.random();
  }

}
