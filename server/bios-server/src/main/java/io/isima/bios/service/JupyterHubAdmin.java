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
package io.isima.bios.service;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.errors.exception.InvalidValueException;
import io.isima.bios.errors.exception.JupyterHubSignupFailedException;
import io.isima.bios.errors.exception.TfosException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.core.Response.Status;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JupyterHubAdmin {
  private static Logger logger = LoggerFactory.getLogger(JupyterHubAdmin.class);
  private static ObjectMapper mapper =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  private static String HUB_AUTHORIZATION_TOKEN = TfosConfig.getJupyterHubAdminToken();
  private static String HUB_URL = TfosConfig.getJupyterHubUrl();
  private static String HUB_API_PREFIX = "/hub/api";
  private static String ADD_USERS = "/users";
  private static String USER_TOKEN = "/tokens";
  private static String SERVER = "/server";
  private static OkHttpClient client;

  public JupyterHubAdmin() {
    client = createOkHttpClient();
  }

  private OkHttpClient createOkHttpClient() {
    try {
      final TrustManager[] trustAllCerts = new TrustManager[] {new X509TrustManagerImpl()};

      final SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
      sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
      final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

      OkHttpClient.Builder builder = new OkHttpClient.Builder();
      builder.sslSocketFactory(sslSocketFactory, (X509TrustManager) trustAllCerts[0]);
      if (TfosConfig.useSelfSignedCertForJupyterHub()) {
        builder.hostnameVerifier(new SelfSignedHostNameVerifier());
      }
      OkHttpClient okHttpClient = builder.build();
      return okHttpClient;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void createUser(String email) throws TfosException {
    String json = "{\"admin\": false, \"usernames\": [\"" + email + "\"]}";
    Response response = null;
    ResponseBody responseBody = null;
    Request request =
        new Request.Builder()
            .url(HUB_URL + HUB_API_PREFIX + ADD_USERS)
            .addHeader("Authorization", HUB_AUTHORIZATION_TOKEN)
            .addHeader("Content-Type", "application/json")
            .post(RequestBody.create(MediaType.parse("application/json"), json))
            .build();
    try {
      Call call = client.newCall(request);
      response = call.execute();
      if (response.code() / 100 != 2) {
        final var status = Status.fromStatusCode(response.code());
        responseBody = response.body();
        throw new JupyterHubSignupFailedException(
            status,
            "Failed to create a user; email=%s, status=%d, error=%s",
            email,
            response.code(),
            responseBody.string());
      }
    } catch (IOException e) {
      throw new TfosException(
          String.format("Something went wrong with hub user creation; user=%s", email), e);
    } finally {
      if (response != null) {
        response.close();
      }
      if (responseBody != null) {
        responseBody.close();
      }
    }
  }

  public String encodeEmail(String email) throws InvalidValueException {
    String[] emailParts = email.split("@");
    try {
      return URLEncoder.encode(emailParts[0], StandardCharsets.UTF_8.toString())
          + "@"
          + URLEncoder.encode(emailParts[1], StandardCharsets.UTF_8.toString());
    } catch (UnsupportedEncodingException e) {
      throw new InvalidValueException("Failed to encode email for JupyterHub", e);
    }
  }

  public String createTokenForUser(String email) throws TfosException {
    Response response = null;
    ResponseBody responseBody = null;
    try {
      String url =
          HUB_URL
              + HUB_API_PREFIX
              + ADD_USERS
              + "/"
              + URLEncoder.encode(email, StandardCharsets.UTF_8.toString())
              + USER_TOKEN;
      logger.info("Creating token with url: {}", url);
      Request request =
          new Request.Builder()
              .url(url)
              .addHeader("Authorization", HUB_AUTHORIZATION_TOKEN)
              .addHeader("Content-Type", "application/json")
              .post(RequestBody.create(MediaType.parse("application/json"), ""))
              .build();
      Call call = client.newCall(request);
      response = call.execute();
      responseBody = response.body();
      if (response.code() / 100 != 2) {
        final var status = Status.fromStatusCode(response.code());
        throw new JupyterHubSignupFailedException(
            status,
            "Creating Jupyterhub token failed; status=%d, message=%s",
            response.code(),
            responseBody);
      }
      String jsonResponse = responseBody.string();
      UserToken ut = mapper.readValue(jsonResponse, UserToken.class);
      return ut.getToken();
    } catch (IOException ioe) {
      logger.error("Error while generating hub token for user: {}", email, ioe);
      throw new TfosException(ioe.toString());
    } finally {
      if (response != null) {
        response.close();
      }
      if (responseBody != null) {
        responseBody.close();
      }
    }
  }

  public void startNotebookServerForUser(String email, String hubKey) throws TfosException {
    Response response = null;
    try {
      String url =
          HUB_URL
              + HUB_API_PREFIX
              + ADD_USERS
              + "/"
              + URLEncoder.encode(email, StandardCharsets.UTF_8.toString())
              + SERVER;
      logger.info("Starting server at url: {}", url);
      Request request =
          new Request.Builder()
              .url(url)
              .addHeader("Authorization", "token " + hubKey)
              .addHeader("Content-Type", "application/json")
              .post(RequestBody.create(MediaType.parse("application/json"), ""))
              .build();
      Call call = client.newCall(request);
      response = call.execute();
    } catch (IOException ioe) {
      logger.error("Error while starting notebook server for user: {}", email, ioe);
      throw new TfosException(ioe.toString());
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }

  static class UserToken {
    @JsonProperty(value = "user")
    private String user;

    @JsonProperty(value = "id")
    private String id;

    @JsonProperty(value = "kind")
    private String kind;

    @JsonProperty(value = "created")
    private String created;

    @JsonProperty(value = "last_activity")
    private String lastActivity;

    @JsonProperty(value = "expires_at")
    private String expiresAt;

    @JsonProperty(value = "note")
    private String note;

    @JsonProperty(value = "token")
    private String token;

    public UserToken() {}

    public String getUser() {
      return user;
    }

    public void setUser(final String user) {
      this.user = user;
    }

    public String getId() {
      return id;
    }

    public void setId(final String id) {
      this.id = id;
    }

    public String getKind() {
      return kind;
    }

    public void setKind(final String kind) {
      this.kind = kind;
    }

    public String getCreated() {
      return created;
    }

    public void setCreated(final String created) {
      this.created = created;
    }

    public String getLastActivity() {
      return lastActivity;
    }

    public void setLastActivity(final String lastActivity) {
      this.lastActivity = lastActivity;
    }

    public String getExpiresAt() {
      return expiresAt;
    }

    public void setExpiresAt(final String expiresAt) {
      this.expiresAt = expiresAt;
    }

    public String getNote() {
      return note;
    }

    public void setNote(final String note) {
      this.note = note;
    }

    public String getToken() {
      return token;
    }

    public void setToken(final String token) {
      this.token = token;
    }
  }
}
