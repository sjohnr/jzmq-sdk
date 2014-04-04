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
import org.zeromq.messaging.ZmqContext;
import org.zeromq.messaging.ZmqMessage;
import org.zeromq.support.thread.ZmqRunnable;

import java.util.Comparator;

import static java.util.Arrays.asList;

class ServiceFixture extends BaseFixture {

  void workerEmitter(ZmqContext ctx,
                     ZmqMessageProcessor messageProcessor,
                     String... connectAddresses) {
    with(
        ZmqRunnable.builder()
                   .withRunnableContext(
                       WorkerAnonymEmitter.builder()
                                          .withCtx(ctx)
                                          .withProps(Props.builder()
                                                          .withConnectAddr(asList(connectAddresses))
                                                          .build())
                                          .withMessageProcessor(messageProcessor)
                                          .withPollTimeout(10)
                                          .build()
                   )
                   .build()
    );
  }

  void workerEmitterWithId(ZmqContext ctx,
                           String id,
                           ZmqMessageProcessor messageProcessor,
                           String... connectAddresses) {
    with(
        ZmqRunnable.builder()
                   .withRunnableContext(
                       WorkerAnonymEmitter.builder()
                                          .withCtx(ctx)
                                          .withProps(Props.builder()
                                                          .withIdentityPrefix(id)
                                                          .withConnectAddr(asList(connectAddresses))
                                                          .build())
                                          .withMessageProcessor(messageProcessor)
                                          .withPollTimeout(10)
                                          .build()
                   )
                   .build()
    );
  }

  void workerAcceptor(ZmqContext ctx,
                      ZmqMessageProcessor messageProcessor,
                      String... connectAddresses) {
    with(
        ZmqRunnable.builder()
                   .withRunnableContext(
                       WorkerAnonymAcceptor.builder()
                                           .withCtx(ctx)
                                           .withProps(Props.builder()
                                                           .withConnectAddr(asList(connectAddresses))
                                                           .build())
                                           .withMessageProcessor(messageProcessor)
                                           .build()
                   )
                   .build()
    );
  }

  void workerWellknown(ZmqContext ctx,
                       String bindAddress,
                       ZmqMessageProcessor messageProcessor) {
    with(
        ZmqRunnable.builder()
                   .withRunnableContext(
                       WorkerWellknown.builder()
                                      .withCtx(ctx)
                                      .withProps(Props.builder()
                                                      .withBindAddr(bindAddress)
                                                      .build())
                                      .withMessageProcessor(messageProcessor)
                                      .build()
                   )
                   .build()
    );
  }

  void lruRouter(ZmqContext ctx,
                 String frontendAddress,
                 String backendAddress,
                 LruCache lruCache) {
    with(
        ZmqRunnable.builder()
                   .withRunnableContext(
                       LruRouter.builder()
                                .withCtx(ctx)
                                .withSocketIdentityStorage(lruCache)
                                .withFrontendProps(Props.builder()
                                                        .withBindAddr(frontendAddress)
                                                        .build())
                                .withBackendProps(Props.builder()
                                                       .withBindAddr(backendAddress)
                                                       .build())
                                .build()
                   )
                   .build()
    );
  }

  void fairRouter(ZmqContext ctx,
                  String frontendAddress,
                  String backendAddress) {
    with(
        ZmqRunnable.builder()
                   .withRunnableContext(
                       FairRouter.builder()
                                 .withCtx(ctx)
                                 .withFrontendProps(Props.builder()
                                                         .withBindAddr(frontendAddress)
                                                         .build())
                                 .withBackendProps(Props.builder()
                                                        .withBindAddr(backendAddress)
                                                        .build())
                                 .build()
                   )
                   .build()
    );
  }

  void fairActiveAcceptor(ZmqContext ctx,
                          String frontendAddress,
                          String backendAddress) {
    with(
        ZmqRunnable.builder()
                   .withRunnableContext(
                       FairActiveAcceptor.builder()
                                         .withCtx(ctx)
                                         .withFrontendProps(Props.builder()
                                                                 .withConnectAddr(frontendAddress)
                                                                 .build())
                                         .withBackendProps(Props.builder()
                                                                .withBindAddr(backendAddress)
                                                                .build())
                                         .build()
                   )
                   .build()
    );
  }

  void fairPassiveAcceptor(ZmqContext ctx,
                           String frontendAddress,
                           String backendAddress) {
    with(
        ZmqRunnable.builder()
                   .withRunnableContext(
                       FairPassiveAcceptor.builder()
                                          .withCtx(ctx)
                                          .withFrontendProps(Props.builder()
                                                                  .withConnectAddr(frontendAddress)
                                                                  .build())
                                          .withBackendProps(Props.builder()
                                                                 .withBindAddr(backendAddress)
                                                                 .build())
                                          .build()
                   )
                   .build()
    );
  }

  void fairEmitter(ZmqContext ctx,
                   String frontendAddress,
                   String backendAddress) {
    with(
        ZmqRunnable.builder()
                   .withRunnableContext(
                       FairEmitter.builder()
                                  .withCtx(ctx)
                                  .withFrontendProps(Props.builder()
                                                          .withBindAddr(frontendAddress)
                                                          .build())
                                  .withBackendProps(Props.builder()
                                                         .withConnectAddr(backendAddress)
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
      return ZmqMessage.builder(MSG).withPayload(ANSWER.payload()).build();
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

  ZmqCaller newBindingCaller(ZmqContext ctx, String bindAddress) {
    ZmqCaller target = ZmqCaller.builder(ctx)
                                .withChannelProps(Props.builder()
                                                       .withBindAddr(bindAddress)
                                                       .build())
                                .build();

    with(target);
    return target;
  }

  ZmqCaller newConnectingCaller(ZmqContext ctx, String... connAddresses) {
    ZmqCaller target = ZmqCaller.builder(ctx)
                                .withChannelProps(Props.builder()
                                                       .withConnectAddr(asList(connAddresses))
                                                       .build())
                                .build();
    with(target);
    return target;
  }

  ZmqCaller newConnectingCallerWithId(ZmqContext ctx,
                                      String id,
                                      String... connAddresses) {
    ZmqCaller target = ZmqCaller.builder(ctx)
                                .withChannelProps(
                                    Props.builder()
                                         .withConnectAddr(asList(connAddresses))
                                         .withIdentityPrefix(id)
                                         .build())
                                .build();
    with(target);
    return target;
  }
}
