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
#include <map>
#include <set>

#include "gtest/gtest.h"
#include "tfos-common/message_handler.h"
#include "tfos-common/object_mapper.h"
#include "tfos-dapi/dapi_modules.h"
#include "tfos-physical/connection_manager.h"
#include "tfoscsdk/status.h"

namespace tfos::csdk {

class DefaultMessageHandler;

class ModulesTest : public ::testing::Test {
 protected:
  const ObjectMapper *object_mapper;

  void SetUp() { object_mapper = new ObjectMapper(); }

  void TearDown() { delete object_mapper; }
};

TEST_F(ModulesTest, ConnectionManagerFactory) {
  ConnectionManager *default_manager = ConnectionManagerFactory::Create("", object_mapper);
  ASSERT_NE(default_manager, nullptr);
  EXPECT_EQ(default_manager->name(), "basic");
  delete default_manager;

  ConnectionManager *mock_manager = ConnectionManagerFactory::Create("mock", object_mapper);
  ASSERT_NE(mock_manager, nullptr);
  EXPECT_EQ(mock_manager->name(), "mock");
  delete mock_manager;

  ConnectionManager *unknown_manager =
      ConnectionManagerFactory::Create("nosuchimpl", object_mapper);
  EXPECT_EQ(unknown_manager, nullptr);
  delete unknown_manager;
}

TEST_F(ModulesTest, MessageHandlerFactory) {
  MessageHandler *default_handler = MessageHandlerFactory::Create("", object_mapper, nullptr);
  ASSERT_NE(default_handler, nullptr);
  EXPECT_EQ(default_handler->name(), "basic");
  delete default_handler;

  MessageHandler *mock_handler = MessageHandlerFactory::Create("mock", object_mapper, nullptr);
  ASSERT_NE(mock_handler, nullptr);
  EXPECT_EQ(mock_handler->name(), "mock");
  delete mock_handler;

  MessageHandler *unknown_handler =
      MessageHandlerFactory::Create("nosuchimpl", object_mapper, nullptr);
  EXPECT_EQ(unknown_handler, nullptr);
  delete unknown_handler;
}

static void DapiModuleTest(const std::string &message_handler_name,
                           const std::string &expected_handler_name) {
  capi_t id = DirectApiModules::Create(message_handler_name);
  ASSERT_GE(id, 0);
  DirectApiModules *modules = DirectApiModules::Get(id, nullptr, nullptr);
  ASSERT_NE(modules, nullptr);
  EXPECT_NE(modules->session_controller(), nullptr);
  ASSERT_NE(modules->message_handler(), nullptr);
  EXPECT_EQ(modules->message_handler()->name(), expected_handler_name);
  // TODO(Naoki): Test DirectApiModules::Get()

  DirectApiModules::Terminate(id);
}

TEST_F(ModulesTest, DapiModulesDefault) { DapiModuleTest("", "basic"); }

TEST_F(ModulesTest, DapiModulesMock) { DapiModuleTest("mock", "mock"); }

void cb(status_code_t status_code, completion_data_t *completion_data, payload_t response_data,
    void *cb_args) {
  status_code_t *result = reinterpret_cast<status_code_t *>(cb_args);
  *result = status_code;
}

TEST_F(ModulesTest, DapiModulesInvalid) {
  capi_t id = DirectApiModules::Create("unknown");
  ASSERT_LT(id, 0);
  DirectApiModules *modules = DirectApiModules::Get(id, nullptr, nullptr);
  ASSERT_EQ(modules, nullptr);

  status_code_t status_code;
  EXPECT_EQ(DirectApiModules::Get(id, cb, &status_code), nullptr);
  EXPECT_EQ(status_code, Statuses::StatusCode(OpStatus::GENERIC_CLIENT_ERROR));

  DirectApiModules::Terminate(id);
}

TEST_F(ModulesTest, MultipleModuleSets) {
  std::map<int, DirectApiModules *> created;
  for (int i = 0; i < 16; ++i) {
    capi_t id = DirectApiModules::Create("");
    ASSERT_GE(id, 0);
    ASSERT_EQ(created.find(id), created.end());
    if (i == 7) {
      // try failing
      capi_t id_fail = DirectApiModules::Create("unknown");
      ASSERT_LT(id_fail, 0);
    }
    // check the new instance is different from the old ones, also check no
    // existing instances have been modified.
    auto new_instance = DirectApiModules::Get(id, nullptr, nullptr);
    for (auto entry : created) {
      auto created_id = entry.first;
      auto instance = entry.second;
      ASSERT_NE(new_instance, instance);
      ASSERT_EQ(DirectApiModules::Get(created_id, nullptr, nullptr), instance);
    }
    created[id] = new_instance;
  }
  capi_t id_overflow = DirectApiModules::Create("");
  ASSERT_LT(id_overflow, 0);

  // check no mod
  for (auto entry : created) {
    auto created_id = entry.first;
    auto instance = entry.second;
    ASSERT_EQ(DirectApiModules::Get(created_id, nullptr, nullptr), instance);
  }

  capi_t id = 5;
  ASSERT_NE(created.find(id), created.end());
  DirectApiModules::Terminate(id);
  ASSERT_EQ(DirectApiModules::Get(id, nullptr, nullptr), nullptr);
  created.erase(id);

  capi_t id2 = DirectApiModules::Create("");
  ASSERT_EQ(id2, id);
  created[id2] = DirectApiModules::Get(id2, nullptr, nullptr);
  ASSERT_NE(created[id2], nullptr);

  ASSERT_LT(DirectApiModules::Create(""), 0);

  capi_t id3 = 3;
  capi_t id7 = 7;

  DirectApiModules::Terminate(id7);
  DirectApiModules::Terminate(id3);

  created.erase(id3);
  created.erase(id7);

  capi_t idx = DirectApiModules::Create("");
  ASSERT_EQ(created.find(idx), created.end());
  capi_t idy = DirectApiModules::Create("");
  ASSERT_EQ(created.find(idy), created.end());

  ASSERT_LT(DirectApiModules::Create(""), 0);

  for (auto entry : created) {
    DirectApiModules::Terminate(entry.first);
  }
  DirectApiModules::Terminate(idx);
  DirectApiModules::Terminate(idy);
}

}  // namespace tfos::csdk
