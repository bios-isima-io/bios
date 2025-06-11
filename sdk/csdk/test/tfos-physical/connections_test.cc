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
#include <memory>

#include "gtest/gtest.h"
#include "tfos-common/utils.h"
#include "tfos-physical/connection.h"

namespace tfos::csdk {

TEST(ConnectionTest, ParseUrlNormal) {
  std::unique_ptr<Connection> conn(
      Connection::BuildConnection("https://tfos.tieredfractals.com:8443"));
  EXPECT_TRUE(conn);
  EXPECT_EQ(conn->pipe(), nullptr);
  EXPECT_EQ(conn->endpoint(), "tfos.tieredfractals.com:8443:1");
  EXPECT_EQ(conn->host(), "tfos.tieredfractals.com");
  EXPECT_EQ(conn->port(), 8443);
  EXPECT_EQ(conn->protocol(), "https");
  EXPECT_TRUE(conn->ssl_enabled());
  EXPECT_TRUE(conn->IsReady());
  EXPECT_FALSE(conn->is_signal());  // false in default
}

TEST(ConnectionTest, ParseUrlNormalWithoutPort) {
  std::unique_ptr<Connection> conn(Connection::BuildConnection("https://192.168.0.1"));
  EXPECT_TRUE(conn);
  EXPECT_EQ(conn->pipe(), nullptr);
  EXPECT_EQ(conn->endpoint(), "192.168.0.1:443:1");
  EXPECT_EQ(conn->host(), "192.168.0.1");
  EXPECT_EQ(conn->port(), 443);
  EXPECT_EQ(conn->protocol(), "https");
  EXPECT_TRUE(conn->ssl_enabled());
  EXPECT_TRUE(conn->IsReady());
  EXPECT_FALSE(conn->is_signal());  // false in default
}

TEST(ConnectionTest, ParseUrlWithErrorMark) {
  std::unique_ptr<Connection> conn(
      Connection::BuildConnection(".https://tfos.tieredfractals.com:8443"));
  EXPECT_TRUE(conn);
  EXPECT_EQ(conn->pipe(), nullptr);
  EXPECT_EQ(conn->endpoint(), "tfos.tieredfractals.com:8443:1");
  EXPECT_EQ(conn->host(), "tfos.tieredfractals.com");
  EXPECT_EQ(conn->port(), 8443);
  EXPECT_EQ(conn->protocol(), "https");
  EXPECT_TRUE(conn->ssl_enabled());
  EXPECT_FALSE(conn->IsReady());
  EXPECT_FALSE(conn->is_signal());  // false in default
}

TEST(ConnectionTest, ParseUrlHttp) {
  std::unique_ptr<Connection> conn(
      Connection::BuildConnection("http://tfos.tieredfractals.com:8080"));
  EXPECT_TRUE(conn);
  EXPECT_EQ(conn->pipe(), nullptr);
  EXPECT_EQ(conn->endpoint(), "tfos.tieredfractals.com:8080:0");
  EXPECT_EQ(conn->host(), "tfos.tieredfractals.com");
  EXPECT_EQ(conn->port(), 8080);
  EXPECT_EQ(conn->protocol(), "http");
  EXPECT_FALSE(conn->ssl_enabled());
  EXPECT_TRUE(conn->IsReady());
  EXPECT_FALSE(conn->is_signal());  // false in default
}

TEST(ConnectionTest, ParseUrlHttpWithoutPort) {
  std::unique_ptr<Connection> conn(Connection::BuildConnection("http://localhost"));
  EXPECT_TRUE(conn);
  EXPECT_EQ(conn->pipe(), nullptr);
  EXPECT_EQ(conn->endpoint(), "localhost:80:0");
  EXPECT_EQ(conn->host(), "localhost");
  EXPECT_EQ(conn->port(), 80);
  EXPECT_EQ(conn->protocol(), "http");
  EXPECT_FALSE(conn->ssl_enabled());
  EXPECT_TRUE(conn->IsReady());
  EXPECT_FALSE(conn->is_signal());  // false in default
}

TEST(ConnectionTest, ParseUrlUnknownProtocol) {
  std::unique_ptr<Connection> conn(Connection::BuildConnection("sftp://tfos.tieredfractals.com"));
  EXPECT_TRUE(conn);
  EXPECT_EQ(conn->pipe(), nullptr);
  EXPECT_EQ(conn->protocol(), "sftp");
}

TEST(ConnectionTest, ParseUrlInvalidSyntax) {
  std::unique_ptr<Connection> conn(Connection::BuildConnection("this is not a URL"));
  EXPECT_FALSE(conn);
}

TEST(ConnectionTest, ParseUrlEndsWithSlash) {
  std::unique_ptr<Connection> conn(
      Connection::BuildConnection("https://tfos.tieredfractals.com:9443/"));
  EXPECT_TRUE(conn);
  EXPECT_EQ(conn->pipe(), nullptr);
  EXPECT_EQ(conn->endpoint(), "tfos.tieredfractals.com:9443:1");
  EXPECT_EQ(conn->host(), "tfos.tieredfractals.com");
  EXPECT_EQ(conn->port(), 9443);
  EXPECT_EQ(conn->protocol(), "https");
  EXPECT_TRUE(conn->ssl_enabled());
  EXPECT_TRUE(conn->IsReady());
  EXPECT_FALSE(conn->is_signal());  // false in default
}

TEST(ConnectionTest, ParseUrlEndsWithSlashWithoutPort) {
  std::unique_ptr<Connection> conn(Connection::BuildConnection("https://tfos.tieredfractals.com/"));
  EXPECT_TRUE(conn);
  EXPECT_EQ(conn->pipe(), nullptr);
  EXPECT_EQ(conn->endpoint(), "tfos.tieredfractals.com:443:1");
  EXPECT_EQ(conn->host(), "tfos.tieredfractals.com");
  EXPECT_EQ(conn->port(), 443);
  EXPECT_EQ(conn->protocol(), "https");
  EXPECT_TRUE(conn->ssl_enabled());
  EXPECT_TRUE(conn->IsReady());
  EXPECT_FALSE(conn->is_signal());  // false in default
}

TEST(ConnectionTest, ParseUrlRedundant) {
  std::unique_ptr<Connection> conn(Connection::BuildConnection("https://example.com:9443/hello"));
  EXPECT_TRUE(conn);
  EXPECT_EQ(conn->pipe(), nullptr);
  EXPECT_EQ(conn->endpoint(), "example.com:9443:1");
  EXPECT_EQ(conn->host(), "example.com");
  EXPECT_EQ(conn->port(), 9443);
  EXPECT_EQ(conn->protocol(), "https");
  EXPECT_TRUE(conn->ssl_enabled());
  EXPECT_TRUE(conn->IsReady());
  EXPECT_FALSE(conn->is_signal());  // false in default
}

TEST(ConnectionTest, ParseUrlRedundantWithoutPort) {
  std::unique_ptr<Connection> conn(Connection::BuildConnection("https://example.com/hello"));
  EXPECT_TRUE(conn);
  EXPECT_EQ(conn->pipe(), nullptr);
  EXPECT_EQ(conn->endpoint(), "example.com:443:1");
  EXPECT_EQ(conn->host(), "example.com");
  EXPECT_EQ(conn->port(), 443);
  EXPECT_EQ(conn->protocol(), "https");
  EXPECT_TRUE(conn->ssl_enabled());
  EXPECT_TRUE(conn->IsReady());
  EXPECT_FALSE(conn->is_signal());  // false in default
}

TEST(ConnectionTest, ParseUrlUpperCase) {
  std::unique_ptr<Connection> conn(Connection::BuildConnection("HTTPS://ISIMA.IO:7443"));
  EXPECT_TRUE(conn);
  EXPECT_EQ(conn->pipe(), nullptr);
  EXPECT_EQ(conn->endpoint(), "isima.io:7443:1");
  EXPECT_EQ(conn->host(), "isima.io");
  EXPECT_EQ(conn->port(), 7443);
  EXPECT_EQ(conn->protocol(), "https");
  EXPECT_TRUE(conn->ssl_enabled());
  EXPECT_TRUE(conn->IsReady());
  EXPECT_FALSE(conn->is_signal());  // false in default
}

TEST(ConnectionTest, ErrorMarkExpiry) {
  std::unique_ptr<Connection> conn(
      Connection::BuildConnection(".http://tfos.tieredfractals.com:8080"));
  int64_t start = Utils::CurrentTimeMillis();
  conn->set_error_expiry(500);

  ASSERT_TRUE(conn->IsMarked());
  ASSERT_FALSE(conn->IsReady());

  // sleep for 70ms and nothing should be changed
  int64_t now = Utils::CurrentTimeMillis();
  int64_t sleepTime = (70 - (now - start)) * 1000;

  usleep(sleepTime);
  ASSERT_TRUE(conn->IsMarked());
  bool is_ready = conn->IsReady();
  ASSERT_FALSE(is_ready);

  // sleep until the connection is ready to retry
  usleep(600000);

  // error mark should be still on
  ASSERT_TRUE(conn->IsMarked());
  // but the error should be expired
  ASSERT_TRUE(conn->IsReady());

  // set error mark again
  start = Utils::CurrentTimeMillis();

  conn->UpdateServerMark(true);
  now = Utils::CurrentTimeMillis();
  sleepTime = (70 - (now - start)) * 1000;
  usleep(sleepTime);

  ASSERT_TRUE(conn->IsMarked());
  ASSERT_FALSE(conn->IsReady());

  usleep(600000);

  ASSERT_TRUE(conn->IsMarked());
  ASSERT_TRUE(conn->IsReady());

  // dropping the error mark should be reflected immediately
  conn->UpdateServerMark(true);
  conn->UpdateServerMark(false);
  ASSERT_FALSE(conn->IsMarked());
  ASSERT_TRUE(conn->IsReady());
}

TEST(ConnectionTest, FailureExpiry) {
  std::unique_ptr<Connection> conn(
      Connection::BuildConnection("http://tfos.tieredfractals.com:8080"));
  conn->set_error_expiry(1000);

  ASSERT_FALSE(conn->IsMarked());
  ASSERT_TRUE(conn->IsReady());

  // set failure
  conn->UpdateErrorStatus(true);
  usleep(700000);
  ASSERT_FALSE(conn->IsMarked());
  ASSERT_FALSE(conn->IsReady());

  // sleep for 700ms and the failure state should be expired
  usleep(700000);
  ASSERT_FALSE(conn->IsMarked());
  ASSERT_TRUE(conn->IsReady());

  // set failure again
  conn->UpdateErrorStatus(true);
  usleep(700000);
  // set error mark
  conn->UpdateServerMark(true);
  ASSERT_TRUE(conn->IsMarked());
  ASSERT_FALSE(conn->IsReady());

  // failure state has expired but connection is not ready due to the error mark
  usleep(700000);
  ASSERT_TRUE(conn->IsMarked());
  ASSERT_FALSE(conn->IsReady());

  usleep(700000);
  ASSERT_TRUE(conn->IsMarked());
  ASSERT_TRUE(conn->IsReady());

  // dropping the failure status should take effect immediately
  conn->UpdateServerMark(false);
  conn->UpdateErrorStatus(true);
  ASSERT_FALSE(conn->IsMarked());
  ASSERT_FALSE(conn->IsReady());

  conn->UpdateErrorStatus(false);
  ASSERT_FALSE(conn->IsMarked());
  ASSERT_TRUE(conn->IsReady());

  // dropping the failure status should also drop the server error mark
  conn->UpdateServerMark(true);
  conn->UpdateErrorStatus(true);
  ASSERT_TRUE(conn->IsMarked());
  ASSERT_FALSE(conn->IsReady());

  conn->UpdateErrorStatus(false);
  ASSERT_FALSE(conn->IsMarked());
  ASSERT_TRUE(conn->IsReady());
}

}  // namespace tfos::csdk
