/*
 * Copyright (c) 2012 artem.vysochyn@gmail.com
 * Copyright (c) 2013 Other contributors as noted in the AUTHORS file
 *
 * jzmq-sdk is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation; either version 3 of the License, or
 * (at your option) any later version.
 *
 * jzmq-sdk is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * jzmq-sdk became possible because of jzmq binding and zmq library itself.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.zeromq.messaging.device.service;

import org.zeromq.messaging.BaseFixture;
import org.zeromq.messaging.Props;
import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqContext;
import org.zeromq.messaging.ZmqMessage;
import org.zeromq.support.thread.ZmqRunnable;

import java.util.Comparator;

import static java.util.Arrays.asList;

class ServiceFixture extends BaseFixture {

  void workerEmitter(ZmqContext zmqContext,
                     ZmqMessageProcessor messageProcessor,
                     String... connectAddresses) {
    with(
        ZmqRunnable.builder()
                   .withRunnableContext(
                       WorkerAnonymEmitter.builder()
                                          .withZmqContext(zmqContext)
                                          .withProps(Props.builder()
                                                          .withConnectAddresses(asList(connectAddresses))
                                                          .build())
                                          .withMessageProcessor(messageProcessor)
                                          .withPollTimeout(10)
                                          .build()
                   )
                   .build()
    );
  }

  void workerEmitterWithIdentity(ZmqContext zmqContext,
                                 String id,
                                 ZmqMessageProcessor messageProcessor,
                                 String... connectAddresses) {
    with(
        ZmqRunnable.builder()
                   .withRunnableContext(
                       WorkerAnonymEmitter.builder()
                                          .withZmqContext(zmqContext)
                                          .withProps(Props.builder()
                                                          .withSocketIdPrefix(id)
                                                          .withConnectAddresses(asList(connectAddresses))
                                                          .build())
                                          .withMessageProcessor(messageProcessor)
                                          .withPollTimeout(10)
                                          .build()
                   )
                   .build()
    );
  }

  void workerAcceptor(ZmqContext zmqContext,
                      ZmqMessageProcessor messageProcessor,
                      String... connectAddresses) {
    with(
        ZmqRunnable.builder()
                   .withRunnableContext(
                       WorkerAnonymAcceptor.builder()
                                           .withZmqContext(zmqContext)
                                           .withProps(Props.builder()
                                                           .withConnectAddresses(asList(connectAddresses))
                                                           .build())
                                           .withMessageProcessor(messageProcessor)
                                           .build()
                   )
                   .build()
    );
  }

  void workerWellknown(ZmqContext zmqContext,
                       String bindAddress,
                       ZmqMessageProcessor messageProcessor) {
    with(
        ZmqRunnable.builder()
                   .withRunnableContext(
                       WorkerWellknown.builder()
                                      .withZmqContext(zmqContext)
                                      .withProps(Props.builder()
                                                      .withBindAddress(bindAddress)
                                                      .build())
                                      .withMessageProcessor(messageProcessor)
                                      .build()
                   )
                   .build()
    );
  }

  void lruRouter(ZmqContext zmqContext,
                 String frontendAddress,
                 String backendAddress,
                 LruCache lruCache) {
    with(
        ZmqRunnable.builder()
                   .withRunnableContext(
                       LruRouter.builder()
                                .withZmqContext(zmqContext)
                                .withSocketIdentityStorage(lruCache)
                                .withFrontendProps(Props.builder()
                                                        .withBindAddress(frontendAddress)
                                                        .build())
                                .withBackendProps(Props.builder()
                                                       .withBindAddress(backendAddress)
                                                       .build())
                                .build()
                   )
                   .build()
    );
  }

  void fairRouter(ZmqContext zmqContext,
                  String frontendAddress,
                  String backendAddress) {
    with(
        ZmqRunnable.builder()
                   .withRunnableContext(
                       FairRouter.builder()
                                 .withZmqContext(zmqContext)
                                 .withFrontendProps(Props.builder()
                                                         .withBindAddress(frontendAddress)
                                                         .build())
                                 .withBackendProps(Props.builder()
                                                        .withBindAddress(backendAddress)
                                                        .build())
                                 .build()
                   )
                   .build()
    );
  }

  void fairActiveAcceptor(ZmqContext zmqContext,
                          String frontendAddress,
                          String backendAddress) {
    with(
        ZmqRunnable.builder()
                   .withRunnableContext(
                       FairActiveAcceptor.builder()
                                         .withZmqContext(zmqContext)
                                         .withFrontendProps(Props.builder()
                                                                 .withConnectAddress(frontendAddress)
                                                                 .build())
                                         .withBackendProps(Props.builder()
                                                                .withBindAddress(backendAddress)
                                                                .build())
                                         .build()
                   )
                   .build()
    );
  }

  void fairPassiveAcceptor(ZmqContext zmqContext,
                           String frontendAddress,
                           String backendAddress) {
    with(
        ZmqRunnable.builder()
                   .withRunnableContext(
                       FairPassiveAcceptor.builder()
                                          .withZmqContext(zmqContext)
                                          .withFrontendProps(Props.builder()
                                                                  .withConnectAddress(frontendAddress)
                                                                  .build())
                                          .withBackendProps(Props.builder()
                                                                 .withBindAddress(backendAddress)
                                                                 .build())
                                          .build()
                   )
                   .build()
    );
  }

  void fairEmitter(ZmqContext zmqContext,
                   String frontendAddress,
                   String backendAddress) {
    with(
        ZmqRunnable.builder()
                   .withRunnableContext(
                       FairEmitter.builder()
                                  .withZmqContext(zmqContext)
                                  .withFrontendProps(Props.builder()
                                                          .withBindAddress(frontendAddress)
                                                          .build())
                                  .withBackendProps(Props.builder()
                                                         .withConnectAddress(backendAddress)
                                                         .build())
                                  .build()
                   )
                   .build()
    );
  }

  static class Answering implements ZmqMessageProcessor {

    private final ZmqMessage ANSWER;

    private Answering(ZmqMessage ANSWER) {
      this.ANSWER = ANSWER;
    }

    public static Answering answering(ZmqMessage ANSWER) {
      return new Answering(ANSWER);
    }

    @Override
    public ZmqMessage process(ZmqMessage MSG) {
      return ZmqMessage.builder(MSG)
                       .withPayload(ANSWER.payload())
                       .build();
    }
  }

  LruCache defaultLruCache() {
    return new LruCache(100);
  }

  LruCache volatileLruCache() {
    return new LruCache(1);
  }

  LruCache notMatchingLruCache() {
    return new LruCache(100,
                        new Comparator<byte[]>() {
                          @Override
                          public int compare(byte[] a, byte[] b) {
                            return -1;
                          }
                        });
  }

  LruCache matchingLRUCache() {
    return new LruCache(100,
                        new Comparator<byte[]>() {
                          @Override
                          public int compare(byte[] front, byte[] back) {
                            return front[0] == back[0] ? 0 : -1;
                          }
                        });
  }

  BlockingClient newBindBlockingClient(ZmqContext zmqContext, String bindAddress) {
    BlockingClient target = BlockingClient.builder()
                                          .withChannelBuilder(
                                              ZmqChannel.builder()
                                                        .withZmqContext(zmqContext)
                                                        .ofDEALERType()
                                                        .withProps(Props.builder()
                                                                        .withBindAddress(bindAddress)
                                                                        .build()))
                                          .build();

    with(target);
    return target;
  }

  BlockingClient newConnBlockingClient(ZmqContext zmqContext, String... connAddresses) {
    BlockingClient target =
        BlockingClient.builder()
                      .withChannelBuilder(ZmqChannel.builder()
                                                    .withZmqContext(zmqContext)
                                                    .ofDEALERType()
                                                    .withProps(Props.builder()
                                                                    .withConnectAddresses(asList(connAddresses))
                                                                    .build()))
                      .build();
    with(target);
    return target;
  }

  BlockingClient newConnBlockingClientWithIdentity(ZmqContext zmqContext,
                                                   String identityPrefix,
                                                   String... connAddresses) {
    BlockingClient target = BlockingClient.builder()
                                          .withChannelBuilder(
                                              ZmqChannel.builder()
                                                        .withZmqContext(zmqContext)
                                                        .ofDEALERType()
                                                        .withProps(Props.builder()
                                                                        .withConnectAddresses(asList(connAddresses))
                                                                        .withSocketIdPrefix(identityPrefix)
                                                                        .build()))
                                          .build();
    with(target);
    return target;
  }
}
