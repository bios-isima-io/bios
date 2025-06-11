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
#include "tfos-physical/impl/http_resource_resolver.h"

#include "gtest/gtest.h"
#include "tfoscsdk/models.h"

namespace tfos::csdk {

class HttpResourceResolverTest : public ::testing::Test {
 protected:
  ResourceResolver *resolver;

  void SetUp() { resolver = new HttpResourceResolver(); }

  void TearDown() { delete resolver; }
};

TEST_F(HttpResourceResolverTest, ResolveGetSignals) {
  Resources resources = {
      .tenant = "abcdefg",
  };
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_GET_SIGNALS), "GET");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_GET_SIGNALS, resources),
            "/bios/v1/tenants/abcdefg/signals");
}

TEST_F(HttpResourceResolverTest, ResolveGetSignalsWithNames) {
  Resources resources = {
      .tenant = "abcdefg",
  };
  resources.options["names"] = "minimum,maximum";
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_GET_SIGNALS), "GET");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_GET_SIGNALS, resources),
            "/bios/v1/tenants/abcdefg/signals?names=minimum,maximum");
}

TEST_F(HttpResourceResolverTest, ResolveGetSignalsWithDetails) {
  Resources resources = {
      .tenant = "abcdefg",
  };
  resources.options["names"] = "[minimum]";
  resources.options["details"] = "false";
  resources.options["includeInternal"] = "false";
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_GET_SIGNALS), "GET");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_GET_SIGNALS, resources),
            "/bios/v1/tenants/abcdefg/"
            "signals?details=false&includeInternal=false&names=[minimum]");
}

TEST_F(HttpResourceResolverTest, ResolveCreateSignal) {
  Resources resources = {
      .tenant = "abcdefg",
  };
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_CREATE_SIGNAL), "POST");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_CREATE_SIGNAL, resources),
            "/bios/v1/tenants/abcdefg/signals");
}

TEST_F(HttpResourceResolverTest, ResolveUpdateSignal) {
  Resources resources = {.tenant = "abcdefg", .stream = "signal_name"};
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_UPDATE_SIGNAL), "POST");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_UPDATE_SIGNAL, resources),
            "/bios/v1/tenants/abcdefg/signals/signal_name");
}

TEST_F(HttpResourceResolverTest, ResolveDeleteSignal) {
  Resources resources = {.tenant = "abcdefg", .stream = "signal_name"};
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_DELETE_SIGNAL), "DELETE");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_DELETE_SIGNAL, resources),
            "/bios/v1/tenants/abcdefg/signals/signal_name");
}

TEST_F(HttpResourceResolverTest, ResolveGetContext) {
  Resources resources = {
      .tenant = "abcdefg",
  };
  resources.options["names"] = "minimum";
  resources.options["details"] = "false";
  resources.options["includeInternal"] = "false";
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_GET_CONTEXTS), "GET");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_GET_CONTEXTS, resources),
            "/bios/v1/tenants/abcdefg/"
            "contexts?details=false&includeInternal=false&names=minimum");
}

TEST_F(HttpResourceResolverTest, ResolveCreateContext) {
  Resources resources = {
      .tenant = "abcdefg",
  };
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_CREATE_CONTEXT), "POST");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_CREATE_CONTEXT, resources),
            "/bios/v1/tenants/abcdefg/contexts");
}

TEST_F(HttpResourceResolverTest, ResolveUpdateContext) {
  Resources resources = {.tenant = "abcdefg", .stream = "context_name"};
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_UPDATE_CONTEXT), "POST");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_UPDATE_CONTEXT, resources),
            "/bios/v1/tenants/abcdefg/contexts/context_name");
}

TEST_F(HttpResourceResolverTest, ResolveDeleteContext) {
  Resources resources = {.tenant = "abcdefg", .stream = "context_name"};
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_DELETE_CONTEXT), "DELETE");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_DELETE_CONTEXT, resources),
            "/bios/v1/tenants/abcdefg/contexts/context_name");
}

TEST_F(HttpResourceResolverTest, ResolveGetTenants) {
  Resources resources = {};
  resources.options["names"] = "tenant1,tenant2";
  resources.options["includeInternal"] = "false";
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_GET_TENANTS_BIOS), "GET");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_GET_TENANTS_BIOS, resources),
            "/bios/v1/tenants?includeInternal=false&names=tenant1,tenant2");
}
TEST_F(HttpResourceResolverTest, ResolveGetTenant) {
  Resources resources = {.tenant = "abcdefg"};
  resources.path_params["tenantName"] = "get_tenant_name";
  resources.options["includeInternal"] = "false";
  resources.options["detail"] = "false";
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_GET_TENANT_BIOS), "GET");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_GET_TENANT_BIOS, resources),
            "/bios/v1/tenants/get_tenant_name?detail=false&includeInternal=false");
}

TEST_F(HttpResourceResolverTest, ResolveCreateTenant) {
  Resources resources = {};
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_CREATE_TENANT_BIOS), "POST");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_CREATE_TENANT_BIOS, resources), "/bios/v1/tenants");
}

TEST_F(HttpResourceResolverTest, ResolveUpdateTenant) {
  Resources resources = {
      .tenant = "tenant_name",
  };
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_UPDATE_TENANT_BIOS), "POST");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_UPDATE_TENANT_BIOS, resources),
            "/bios/v1/tenants/tenant_name");
}

TEST_F(HttpResourceResolverTest, ResolveDeleteTenant) {
  Resources resources = {
      .tenant = "abcdefg",
  };
  resources.path_params["tenantName"] = "tenant_name";
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_DELETE_TENANT_BIOS), "DELETE");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_DELETE_TENANT_BIOS, resources),
            "/bios/v1/tenants/tenant_name");
}

TEST_F(HttpResourceResolverTest, ResolveMultiGetContextEntries) {
  Resources resources = {
      .tenant = "abcdefg",
  };
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_MULTI_GET_CONTEXT_ENTRIES_BIOS), "POST");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_MULTI_GET_CONTEXT_ENTRIES_BIOS, resources),
            "/bios/v1/tenants/abcdefg/multi-get");
}

TEST_F(HttpResourceResolverTest, ResolveGetContextEntries) {
  Resources resources = {
      .tenant = "abcdefg",
      .stream = "context_name",
  };
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_GET_CONTEXT_ENTRIES_BIOS), "POST");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_GET_CONTEXT_ENTRIES_BIOS, resources),
            "/bios/v1/tenants/abcdefg/contexts/context_name/entries/fetch");
}

TEST_F(HttpResourceResolverTest, ResolveSelectContextEntries) {
  Resources resources = {
      .tenant = "test_tenant",
      .stream = "context_name",
  };
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_SELECT_CONTEXT_ENTRIES), "POST");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_SELECT_CONTEXT_ENTRIES, resources),
            "/bios/v1/tenants/test_tenant/contexts/context_name/entries/select");
}

TEST_F(HttpResourceResolverTest, ResolveCreateContextEntry) {
  Resources resources = {
      .tenant = "abcdefg",
      .stream = "context_name",
  };
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_CREATE_CONTEXT_ENTRY_BIOS), "POST");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_CREATE_CONTEXT_ENTRY_BIOS, resources),
            "/bios/v1/tenants/abcdefg/contexts/context_name/entries");
}

TEST_F(HttpResourceResolverTest, ResolveUpdateContextEntry) {
  Resources resources = {.tenant = "abcdefg", .stream = "context_name"};
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_UPDATE_CONTEXT_ENTRY_BIOS), "PATCH");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_UPDATE_CONTEXT_ENTRY_BIOS, resources),
            "/bios/v1/tenants/abcdefg/contexts/context_name/entries");
}

TEST_F(HttpResourceResolverTest, ResolveReplaceContextEntry) {
  Resources resources = {.tenant = "abcdefg", .stream = "context_name"};
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_REPLACE_CONTEXT_ENTRY_BIOS), "POST");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_REPLACE_CONTEXT_ENTRY_BIOS, resources),
            "/bios/v1/tenants/abcdefg/contexts/context_name/entries/replace");
}

TEST_F(HttpResourceResolverTest, ResolveDeleteContextEntry) {
  Resources resources = {.tenant = "abcdefg", .stream = "context_name"};
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_DELETE_CONTEXT_ENTRY_BIOS), "POST");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_DELETE_CONTEXT_ENTRY_BIOS, resources),
            "/bios/v1/tenants/abcdefg/contexts/context_name/entries/delete");
}

TEST_F(HttpResourceResolverTest, ResolveLoginBIOS) {
  Resources resources;
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_LOGIN_BIOS), "POST");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_LOGIN_BIOS, resources), "/bios/v1/auth/login");
}

TEST_F(HttpResourceResolverTest, ResolveSelectProto) {
  Resources resources = {
      .tenant = "mytesttenant",
  };
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_SELECT_PROTO), "POST");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_SELECT_PROTO, resources),
            "/bios/v1/tenants/mytesttenant/events/select");
}

TEST_F(HttpResourceResolverTest, ResolveInsertProto) {
  Resources resources = {
      .tenant = "mytenant",
      .stream = "mysignal",
  };
  resources.path_params["eventId"] = "ecb16ec0-ba2c-11ea-bc37-29788aa36584";
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_INSERT_PROTO), "PUT");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_INSERT_PROTO, resources),
            "/bios/v1/tenants/mytenant/signals/mysignal/events/"
            "ecb16ec0-ba2c-11ea-bc37-29788aa36584");
}

TEST_F(HttpResourceResolverTest, ResolveInsertBulkProto) {
  Resources resources = {
      .tenant = "mytenant",
  };
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_INSERT_BULK_PROTO), "POST");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_INSERT_BULK_PROTO, resources),
            "/bios/v1/tenants/mytenant/events/bulk");
}

TEST_F(HttpResourceResolverTest, ResolveCreateExportDestination) {
  Resources resources = {.tenant = "mytenant"};
  resources.path_params["storageName"] = "s3Bucket1";
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_CREATE_EXPORT_DESTINATION), "POST");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_CREATE_EXPORT_DESTINATION, resources),
            "/bios/v1/tenants/mytenant/exports");
}

TEST_F(HttpResourceResolverTest, ResolveGetExportDestination) {
  Resources resources = {.tenant = "mytenant"};
  resources.path_params["storageName"] = "s3Bucket3";
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_GET_EXPORT_DESTINATION), "GET");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_GET_EXPORT_DESTINATION, resources),
            "/bios/v1/tenants/mytenant/exports/s3Bucket3");
}

TEST_F(HttpResourceResolverTest, ResolveUpdateExportDestination) {
  Resources resources = {.tenant = "mytenant"};
  resources.path_params["storageName"] = "s3Bucket1";
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_UPDATE_EXPORT_DESTINATION), "PUT");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_UPDATE_EXPORT_DESTINATION, resources),
            "/bios/v1/tenants/mytenant/exports/s3Bucket1");
}

TEST_F(HttpResourceResolverTest, ResolveDeleteExportDestination) {
  Resources resources = {.tenant = "mytenant"};
  resources.path_params["storageName"] = "s3Bucket1";
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_DELETE_EXPORT_DESTINATION), "DELETE");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_DELETE_EXPORT_DESTINATION, resources),
            "/bios/v1/tenants/mytenant/exports/s3Bucket1");
}

TEST_F(HttpResourceResolverTest, ResolveExportDataStart) {
  Resources resources = {.tenant = "mytenant"};
  resources.path_params["storageName"] = "s3Bucket2";
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_DATA_EXPORT_START), "POST");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_DATA_EXPORT_START, resources),
            "/bios/v1/tenants/mytenant/exports/s3Bucket2/start");
}

TEST_F(HttpResourceResolverTest, ResolveExportDataStop) {
  Resources resources = {.tenant = "mytenant"};
  resources.path_params["storageName"] = "s3Bucket3";
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_DATA_EXPORT_STOP), "POST");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_DATA_EXPORT_STOP, resources),
            "/bios/v1/tenants/mytenant/exports/s3Bucket3/stop");
}

TEST_F(HttpResourceResolverTest, ResolveUpstream) {
  Resources resources;
  EXPECT_EQ(resolver->GetMethod(CSDK_OP_GET_UPSTREAM_CONFIG), "GET");
  EXPECT_EQ(resolver->GetResource(CSDK_OP_GET_UPSTREAM_CONFIG, resources),
            "/bios/v1/admin/upstream");
}

}  // namespace tfos::csdk
