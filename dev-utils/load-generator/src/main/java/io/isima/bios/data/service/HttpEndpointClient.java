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

import io.isima.bios.exception.HttpEndpointException;

import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HttpEndpointClient {
  private final Logger logger = LogManager.getLogger(HttpEndpointException.class);

  private HttpClient httpClient;

  public HttpEndpointClient() {
    httpClient = HttpClientBuilder.create().build();
  }

  public void post(final String endpoint, final String stream, final String eventData) {
    try {
      HttpEntity entity = new StringEntity(eventData);
      HttpPost post = new HttpPost(endpoint);
      post.setEntity(entity);

      HttpResponse response = this.httpClient.execute(post);
      logger.info(
              String.format(endpoint + ", status : %s ", response.getStatusLine().getStatusCode()));
    } catch (IOException ie) {
      throw new HttpEndpointException(ie.getMessage(), ie);
    }
  }

}
