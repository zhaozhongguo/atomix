/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.collection.impl;

import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.collection.AsyncDistributedSet;
import io.atomix.core.collection.DistributedSet;
import io.atomix.core.collection.DistributedSetType;
import io.atomix.primitive.PrimitiveType;

import java.time.Duration;

/**
 * Unmodifiable distributed set.
 */
public class UnmodifiableAsyncDistributedSet<E> extends UnmodifiableAsyncDistributedCollection<E> implements AsyncDistributedSet<E> {
  public UnmodifiableAsyncDistributedSet(AsyncDistributedCollection<E> delegateCollection) {
    super(delegateCollection);
  }

  @Override
  public PrimitiveType type() {
    return DistributedSetType.instance();
  }

  @Override
  public DistributedSet<E> sync(Duration operationTimeout) {
    return new BlockingDistributedSet<>(this, operationTimeout.toMillis());
  }
}
