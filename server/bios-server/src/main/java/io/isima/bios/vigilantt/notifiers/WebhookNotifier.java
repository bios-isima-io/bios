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
package io.isima.bios.vigilantt.notifiers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.models.AlertType;
import io.isima.bios.service.SelfSignedHostNameVerifier;
import io.isima.bios.service.X509TrustManagerImpl;
import io.isima.bios.utils.TfosObjectMapperProvider;
import io.isima.bios.vigilantt.exceptions.NotificationFailedException;
import io.isima.bios.vigilantt.models.NotificationContents;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Set;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import lombok.Setter;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.text.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebhookNotifier implements Notifier {
  private static final Logger logger = LoggerFactory.getLogger(WebhookNotifier.class);
  private static final OkHttpClient httpClient = buildSslHttpClient();
  private static final ObjectMapper mapper = TfosObjectMapperProvider.get();
  private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

  @Setter protected String webhookUrl;

  @Setter protected AlertType alertType;

  public WebhookNotifier(String webhookUrl) {
    this.webhookUrl = webhookUrl;
  }

  /**
   * Build and return an OkHttpClient object that trusts every server. This is useful when the
   * webhook destination does not have a CA-Signed Certificate or it cannot be validated based on
   * the root certificates present on the BIOS server.
   */
  private static OkHttpClient buildSslHttpClient() {
    try {
      final TrustManager[] trustAllCerts = new TrustManager[] {new X509TrustManagerImpl()};
      final SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
      sslContext.init(null, trustAllCerts, new SecureRandom());
      final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();
      final OkHttpClient.Builder builder = new OkHttpClient().newBuilder();
      builder.sslSocketFactory(sslSocketFactory, (X509TrustManager) trustAllCerts[0]);
      if (TfosConfig.useSelfSignedCertForNotifications()) {
        builder.hostnameVerifier(new SelfSignedHostNameVerifier());
      }
      return builder.build();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create OkHttpClient", e);
    }
  }

  @Override
  public void sendNotification(NotificationContents event, String kind, int numRetries)
      throws NotificationFailedException {
    try {
      logger.debug("Dispatching notification {} to webhook url {}", event, webhookUrl);
      String anomalyNotificationJson;
      if (alertType == AlertType.SLACK) {
        anomalyNotificationJson = formatSlackAlertJson(event);
      } else {
        anomalyNotificationJson = mapper.writeValueAsString(event);
      }
      RequestBody body = RequestBody.create(JSON, anomalyNotificationJson);
      Request request = new Request.Builder().url(webhookUrl).post(body).build();
      sendNotificationWithRetries(request, kind, numRetries);
    } catch (JsonProcessingException e) {
      throw new NotificationFailedException("Event object couldn't be converted to JSON:" + e);
    }
  }

  private String formatSlackAlertJson(NotificationContents event) throws JsonProcessingException {
    String notificationJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(event);
    notificationJson = StringEscapeUtils.escapeJson(notificationJson);
    notificationJson =
        "{\"text\": \""
            + "A bi(OS) alert *"
            + event.getAlertName()
            + "* has been raised, "
            + "Please have a look at the following JSON object "
            + "and/or log into the system to get more information - "
            + "<https://"
            + event.getDomainName()
            + ">"
            + "\n```"
            + notificationJson
            + "```\"}";

    return notificationJson;
  }

  private void sendNotificationWithRetries(Request request, String kind, int retryCredit) {
    httpClient
        .newCall(request)
        .enqueue(
            new Callback() {
              private final Set<Integer> retryableStatus = Set.of(404, 408, 500, 502, 503, 504);

              @Override
              public void onFailure(final Call call, final IOException e) {
                final Request request = call.request();
                if (retryCredit > 0) {
                  logger.debug("Retrying notification request {} after exception {}", request, e);
                  sendNotificationWithRetries(request, kind, retryCredit - 1);
                } else {
                  logger.error("Notification request {} failed with exception {}", request, e);
                }
              }

              @Override
              public void onResponse(final Call call, final Response response) throws IOException {
                if (response.isSuccessful()) {
                  logger.debug(
                      "{} notification sent; request={}, response={}", kind, request, response);
                } else {
                  if (retryableStatus.contains(response.code()) && retryCredit > 0) {
                    logger.debug(
                        "Retrying notification request {} after response {}", request, response);
                    sendNotificationWithRetries(request, kind, retryCredit - 1);
                  } else {
                    logger.warn(
                        "{} notification was unsuccessful; request={}, response={}",
                        kind,
                        request,
                        response);
                  }
                }
                response.body().close();
              }
            });
  }
}
