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

import static io.isima.bios.service.Keywords.PATCH;

import io.isima.bios.auth.Auth;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.models.AppType;
import io.isima.bios.models.AttributeTags;
import io.isima.bios.models.UpdateInferredTagsRequest;
import io.isima.bios.models.UserContext;
import io.isima.bios.models.v1.Permission;
import io.isima.bios.server.services.BiosServicePath;
import io.isima.bios.service.handler.FanRouter;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InferredTagsDistributor {
  private static final Logger logger = LoggerFactory.getLogger(InferredTagsDistributor.class);
  private static final String SYSTEM_SCOPE = "/";

  private static final int EXPIRATION_MILLIS = 60 * 1000;

  private final FanRouter<UpdateInferredTagsRequest, Void> fanRouter;
  private final Auth auth;
  private final UserContext userContext;

  public InferredTagsDistributor(Auth auth, HttpClientManager clientManager) {
    this(auth, createFanRouter(clientManager));
  }

  private InferredTagsDistributor(Auth auth, FanRouter<UpdateInferredTagsRequest, Void> fanRouter) {
    this.fanRouter = fanRouter;
    this.auth = auth;
    this.userContext = createUserContext();
  }

  public void doFanRouting(String tenant, String stream, Map<Short, AttributeTags> attributes) {
    final var currentTime = System.currentTimeMillis();
    String currentToken =
        auth.createToken(currentTime, currentTime + EXPIRATION_MILLIS, userContext);
    fanRouter.setSessionToken(currentToken);
    final var request = new UpdateInferredTagsRequest(tenant, stream, attributes);
    try {
      // Synchronous fan routing. Since attribute tag inference is infrequent and not very heavy,
      // we should be fine with a single synchronous thread.
      fanRouter.fanRoute(request, currentTime);
    } catch (TfosException | ApplicationException e) {
      logger.warn("InferredTagsDistributor failed", e);
    }
  }

  private static UserContext createUserContext() {
    UserContext userContext = new UserContext();
    userContext.setScope(SYSTEM_SCOPE);
    userContext.setSubject("n/a");
    userContext.setUserId(TfosConfig.DEFAULT_SUPERADMIN_ID);
    userContext.setOrgId(TfosConfig.DEFAULT_ORG_ID);
    userContext.addPermissions(List.of(Permission.SUPERADMIN, Permission.INTERNAL));
    userContext.setAppName("biOS");
    userContext.setAppType(AppType.REALTIME);
    return userContext;
  }

  private static FanRouter<UpdateInferredTagsRequest, Void> createFanRouter(
      HttpClientManager clientManager) {
    return new HttpFanRouter<>(
        PATCH,
        BiosServicePath.ROOT,
        () -> BiosServicePath.PATH_INFERRED_TAGS,
        null,
        clientManager,
        Void.class);
  }
}
