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
package io.atomix.primitive.proxy.impl;

import com.google.common.collect.Maps;
import io.atomix.primitive.event.EventType;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.proxy.PartitionProxy;
import io.atomix.utils.concurrent.ThreadContext;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static io.atomix.utils.concurrent.Futures.asyncFuture;

/**
 * Raft proxy delegate that completes futures on a thread pool.
 */
public class BlockingAwarePartitionProxy extends DelegatingPartitionProxy {
  private final ThreadContext context;
  private final Map<Consumer<PartitionProxy.State>, Consumer<PartitionProxy.State>> stateChangeListeners = Maps.newConcurrentMap();
  private final Map<Consumer<PrimitiveEvent>, Consumer<PrimitiveEvent>> eventListeners = Maps.newConcurrentMap();

  public BlockingAwarePartitionProxy(PartitionProxy delegate, ThreadContext context) {
    super(delegate);
    this.context = context;
  }

  @Override
  public void addStateChangeListener(Consumer<PartitionProxy.State> listener) {
    Consumer<PartitionProxy.State> wrappedListener = state -> context.execute(() -> listener.accept(state));
    stateChangeListeners.put(listener, wrappedListener);
    super.addStateChangeListener(wrappedListener);
  }

  @Override
  public void removeStateChangeListener(Consumer<PartitionProxy.State> listener) {
    Consumer<PartitionProxy.State> wrappedListener = stateChangeListeners.remove(listener);
    if (wrappedListener != null) {
      super.removeStateChangeListener(wrappedListener);
    }
  }

  @Override
  public CompletableFuture<byte[]> execute(PrimitiveOperation operation) {
    return asyncFuture(super.execute(operation), context);
  }

  @Override
  public void addEventListener(EventType eventType, Consumer<PrimitiveEvent> listener) {
    Consumer<PrimitiveEvent> wrappedListener = e -> context.execute(() -> listener.accept(e));
    eventListeners.put(listener, wrappedListener);
    super.addEventListener(eventType, wrappedListener);
  }

  @Override
  public void removeEventListener(EventType eventType, Consumer<PrimitiveEvent> listener) {
    Consumer<PrimitiveEvent> wrappedListener = eventListeners.remove(listener);
    if (wrappedListener != null) {
      super.removeEventListener(eventType, wrappedListener);
    }
  }
}
