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
package io.atomix.protocols.backup.impl;

import io.atomix.cluster.ClusterService;
import io.atomix.cluster.NodeId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.MessageSubject;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveException.Unavailable;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.primitive.proxy.impl.BlockingAwarePrimitiveProxy;
import io.atomix.primitive.proxy.impl.RecoveringPrimitiveProxy;
import io.atomix.primitive.proxy.impl.RetryingPrimitiveProxy;
import io.atomix.protocols.backup.MultiPrimaryProtocol;
import io.atomix.protocols.backup.PrimaryBackupClient;
import io.atomix.protocols.backup.protocol.MetadataRequest;
import io.atomix.protocols.backup.protocol.MetadataResponse;
import io.atomix.protocols.backup.protocol.PrimaryBackupResponse.Status;
import io.atomix.protocols.backup.proxy.PrimaryBackupProxy;
import io.atomix.protocols.backup.serializer.impl.PrimaryBackupSerializers;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import io.atomix.utils.serializer.Serializer;
import org.slf4j.Logger;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Default primary-backup client.
 */
public class DefaultPrimaryBackupClient implements PrimaryBackupClient {
  private static final Serializer SERIALIZER = PrimaryBackupSerializers.PROTOCOL;

  private final String clientName;
  private final ClusterService clusterService;
  private final ClusterCommunicationService communicationService;
  private final PrimaryElection primaryElection;
  private final ThreadContextFactory threadContextFactory;
  private final ThreadContext threadContext;
  private final MessageSubject metadataSubject;

  public DefaultPrimaryBackupClient(
      String clientName,
      ClusterService clusterService,
      ClusterCommunicationService communicationService,
      PrimaryElection primaryElection,
      ThreadContextFactory threadContextFactory) {
    this.clientName = clientName;
    this.clusterService = clusterService;
    this.communicationService = communicationService;
    this.primaryElection = primaryElection;
    this.threadContextFactory = threadContextFactory;
    this.threadContext = threadContextFactory.createContext();
    this.metadataSubject = new MessageSubject(String.format("%s-metadata", clientName));
  }

  @Override
  @SuppressWarnings("unchecked")
  public PrimitiveProxy.Builder<MultiPrimaryProtocol> proxyBuilder(String primitiveName, PrimitiveType primitiveType, MultiPrimaryProtocol primitiveProtocol) {
    return new ProxyBuilder(primitiveName, primitiveType, primitiveProtocol);
  }

  @Override
  public CompletableFuture<Set<String>> getPrimitives(PrimitiveType primitiveType) {
    CompletableFuture<Set<String>> future = new CompletableFuture<>();
    MetadataRequest request = new MetadataRequest(primitiveType.id());
    threadContext.execute(() -> {
      NodeId primary = primaryElection.getTerm().primary();
      if (primary == null) {
        future.completeExceptionally(new Unavailable());
        return;
      }

      communicationService.<MetadataRequest, MetadataResponse>sendAndReceive(
          metadataSubject,
          request,
          SERIALIZER::encode,
          SERIALIZER::decode,
          primary)
          .whenCompleteAsync((response, error) -> {
            if (error == null) {
              if (response.status() == Status.OK) {
                future.complete(response.primitiveNames());
              } else {
                future.completeExceptionally(new PrimitiveException.Unavailable());
              }
            } else {
              future.completeExceptionally(new PrimitiveException.Unavailable());
            }
          }, threadContext);
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> close() {
    threadContext.close();
    threadContextFactory.close();
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Default primary-backup client builder.
   */
  public static class Builder extends PrimaryBackupClient.Builder {
    @Override
    public PrimaryBackupClient build() {
      Logger log = ContextualLoggerFactory.getLogger(DefaultPrimaryBackupClient.class, LoggerContext.builder(PrimaryBackupClient.class)
          .addValue(clientName)
          .build());
      ThreadContextFactory threadContextFactory = this.threadContextFactory != null
          ? this.threadContextFactory
          : threadModel.factory("backup-client-" + clientName + "-%d", threadPoolSize, log);
      return new DefaultPrimaryBackupClient(
          clientName,
          clusterService,
          communicationService,
          primaryElection,
          threadContextFactory);
    }
  }

  /**
   * Primary-backup proxy builder.
   */
  private class ProxyBuilder extends PrimaryBackupProxy.Builder {
    ProxyBuilder(String name, PrimitiveType primitiveType, MultiPrimaryProtocol primitiveProtocol) {
      super(name, primitiveType, primitiveProtocol);
    }

    @Override
    @SuppressWarnings("unchecked")
    public PrimitiveProxy build() {
      PrimitiveProxy.Builder<MultiPrimaryProtocol> proxyBuilder = new PrimitiveProxy.Builder(name, primitiveType, protocol) {
        @Override
        public PrimitiveProxy build() {
          return new PrimaryBackupProxy(
              clientName,
              name,
              primitiveType,
              clusterService,
              communicationService,
              primaryElection,
              threadContextFactory.createContext());
        }
      }.withMaxRetries(maxRetries)
          .withRetryDelay(retryDelay);

      PrimitiveProxy proxy = new RecoveringPrimitiveProxy(
          clientName,
          name,
          primitiveType,
          proxyBuilder,
          threadContextFactory.createContext());

      // If max retries is set, wrap the client in a retrying proxy client.
      if (maxRetries > 0) {
        proxy = new RetryingPrimitiveProxy(proxy, threadContextFactory.createContext(), maxRetries, retryDelay);
      }

      // Default the executor to use the configured thread pool executor and create a blocking aware proxy client.
      Executor executor = this.executor != null ? this.executor : threadContextFactory.createContext();
      return new BlockingAwarePrimitiveProxy(proxy, executor);
    }
  }
}
