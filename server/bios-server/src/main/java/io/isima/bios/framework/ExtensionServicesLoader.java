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
package io.isima.bios.framework;

import io.isima.bios.server.services.BiosService;
import java.util.List;

/**
 * Interface to load biOS extension services.
 *
 * <p>An extension service is a BiosService class extension that provides a set of API for some
 * special business logic. In order to run a service extension,
 *
 * <dl>
 *   <li>Implement a service, i.e., make a class that extends BiosService.
 *   <li>Create a class that implements this interface. In the {@link #invoke} method, create an
 *       instance of the service and add it to services.
 *   <li>Package the service and the loader into a jar file and place it in a classpath of the biOS
 *       runtime environment.
 *   <li>Set the implemented class name to property <code>io.isima.bios.service.extensions.class
 *       </code> in <code>server.property</code> file.
 *   <li>Restart the biOS server.
 * </dl>
 */
public interface ExtensionServicesLoader {
  /**
   * Invokes extension services.
   *
   * <p>Implement this method to instantiate service extensions and add them to the given <code>
   * services</code> list.
   *
   * @param services List of services where the invokes services are added.
   */
  void invoke(List<BiosService> services);
}
