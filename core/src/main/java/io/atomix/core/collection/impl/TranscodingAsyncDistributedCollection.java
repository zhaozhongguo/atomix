/*
 * Copyright 2016-present Open Networking Foundation
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

package io.atomix.core.collection.impl;

import com.google.common.collect.Maps;
import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.collection.AsyncIterator;
import io.atomix.core.collection.CollectionEvent;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.core.collection.DistributedCollection;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.protocol.PrimitiveProtocol;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An {@code AsyncDistributedCollection} that maps its operations to operations on a
 * differently typed {@code AsyncDistributedCollection} by transcoding operation inputs and outputs.
 *
 * @param <E2> element type of other collection
 * @param <E1> element type of this collection
 */
public class TranscodingAsyncDistributedCollection<E1, E2> implements AsyncDistributedCollection<E1> {

  private final AsyncDistributedCollection<E2> backingCollection;
  private final Function<E1, E2> elementEncoder;
  private final Function<E2, E1> elementDecoder;
  private final Map<CollectionEventListener<E1>, InternalCollectionEventListener> listeners = Maps.newIdentityHashMap();

  public TranscodingAsyncDistributedCollection(
      AsyncDistributedCollection<E2> backingCollection,
      Function<E1, E2> elementEncoder,
      Function<E2, E1> elementDecoder) {
    this.backingCollection = backingCollection;
    this.elementEncoder = k -> k == null ? null : elementEncoder.apply(k);
    this.elementDecoder = k -> k == null ? null : elementDecoder.apply(k);
  }

  @Override
  public String name() {
    return backingCollection.name();
  }

  @Override
  public PrimitiveType type() {
    return backingCollection.type();
  }

  @Override
  public PrimitiveProtocol protocol() {
    return backingCollection.protocol();
  }

  @Override
  public CompletableFuture<Integer> size() {
    return backingCollection.size();
  }

  @Override
  public CompletableFuture<Boolean> add(E1 element) {
    return backingCollection.add(elementEncoder.apply(element));
  }

  @Override
  public CompletableFuture<Boolean> remove(E1 element) {
    return backingCollection.remove(elementEncoder.apply(element));
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return backingCollection.isEmpty();
  }

  @Override
  public CompletableFuture<Void> clear() {
    return backingCollection.clear();
  }

  @Override
  public CompletableFuture<Boolean> contains(E1 element) {
    return backingCollection.contains(elementEncoder.apply(element));
  }

  @Override
  public CompletableFuture<Boolean> addAll(Collection<? extends E1> c) {
    return backingCollection.addAll(c.stream().map(elementEncoder).collect(Collectors.toList()));
  }

  @Override
  public CompletableFuture<Boolean> containsAll(Collection<? extends E1> c) {
    return backingCollection.containsAll(c.stream().map(elementEncoder).collect(Collectors.toList()));
  }

  @Override
  public CompletableFuture<Boolean> retainAll(Collection<? extends E1> c) {
    return backingCollection.retainAll(c.stream().map(elementEncoder).collect(Collectors.toList()));
  }

  @Override
  public CompletableFuture<Boolean> removeAll(Collection<? extends E1> c) {
    return backingCollection.removeAll(c.stream().map(elementEncoder).collect(Collectors.toList()));
  }

  @Override
  public CompletableFuture<AsyncIterator<E1>> iterator() {
    return backingCollection.iterator().thenApply(iterator -> new TranscodingIterator<>(iterator, elementDecoder));
  }

  @Override
  public CompletableFuture<Void> addListener(CollectionEventListener<E1> listener) {
    synchronized (listeners) {
      InternalCollectionEventListener collectionListener =
          listeners.computeIfAbsent(listener, k -> new InternalCollectionEventListener(listener));
      return backingCollection.addListener(collectionListener);
    }
  }

  @Override
  public CompletableFuture<Void> removeListener(CollectionEventListener<E1> listener) {
    synchronized (listeners) {
      InternalCollectionEventListener collectionListener = listeners.remove(listener);
      if (collectionListener != null) {
        return backingCollection.removeListener(collectionListener);
      } else {
        return CompletableFuture.completedFuture(null);
      }
    }
  }

  @Override
  public void addStateChangeListener(Consumer<PrimitiveState> listener) {
    backingCollection.addStateChangeListener(listener);
  }

  @Override
  public void removeStateChangeListener(Consumer<PrimitiveState> listener) {
    backingCollection.removeStateChangeListener(listener);
  }

  @Override
  public DistributedCollection<E1> sync(Duration operationTimeout) {
    return new BlockingDistributedCollection<>(this, operationTimeout.toMillis());
  }

  @Override
  public CompletableFuture<Void> close() {
    return backingCollection.close();
  }

  private class InternalCollectionEventListener implements CollectionEventListener<E2> {
    private final CollectionEventListener<E1> listener;

    InternalCollectionEventListener(CollectionEventListener<E1> listener) {
      this.listener = listener;
    }

    @Override
    public void onEvent(CollectionEvent<E2> event) {
      listener.onEvent(new CollectionEvent<>(event.type(), elementDecoder.apply(event.element())));
    }
  }
}
