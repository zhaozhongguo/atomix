/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.counter.impl;

import io.atomix.core.counter.AtomicCounterConfig;
import io.atomix.core.counter.AtomicCounter;
import io.atomix.core.counter.AtomicCounterBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.primitive.service.ServiceConfig;

import java.util.concurrent.CompletableFuture;

/**
 * Atomic counter proxy builder.
 */
public class AtomicCounterProxyBuilder extends AtomicCounterBuilder {
  public AtomicCounterProxyBuilder(String name, AtomicCounterConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<AtomicCounter> buildAsync() {
    ProxyClient<AtomicCounterService> proxy = protocol().newProxy(
        name(),
        primitiveType(),
        AtomicCounterService.class,
        new ServiceConfig(),
        managementService.getPartitionService());
    return new AtomicCounterProxy(proxy, managementService.getPrimitiveRegistry())
        .connect()
        .thenApply(counter -> counter.sync());
  }
}
