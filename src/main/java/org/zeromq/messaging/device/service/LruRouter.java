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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqException;
import org.zeromq.messaging.ZmqFrames;
import org.zeromq.messaging.ZmqHeaders;
import org.zeromq.messaging.ZmqMessage;
import org.zeromq.messaging.device.ZmqAbstractProxy;
import org.zeromq.messaging.device.ZmqSocketIdentityStorage;

import static org.zeromq.messaging.ZmqException.ErrorCode.HEADER_IS_NOT_SET;
import static org.zeromq.messaging.ZmqException.ErrorCode.SOCKET_IDENTITY_NOT_MATCHED;
import static org.zeromq.messaging.ZmqException.ErrorCode.SOCKET_IDENTITY_STORAGE_IS_EMPTY;

/**
 * LRU device:
 * <pre>
 *   [:f(ROUTER) / :b(ROUTER)]
 * </pre>
 */
public final class LruRouter extends ZmqAbstractProxy {

  private static final Logger LOG = LoggerFactory.getLogger(LruRouter.class);

  public static final class Builder extends ZmqAbstractProxy.Builder<Builder, LruRouter> {

    private Builder() {
      super(new LruRouter());
    }

    public Builder withSocketIdentityStorage(ZmqSocketIdentityStorage socketIdentityStorage) {
      _target.setSocketIdentityStorage(socketIdentityStorage);
      return this;
    }

    @Override
    public void checkInvariant() {
      super.checkInvariant();
      if (_target.socketIdentityStorage == null) {
        throw ZmqException.fatal();
      }
      if (_target.frontendProps.getBindAddresses().isEmpty()) {
        throw ZmqException.fatal();
      }
      if (_target.backendProps.getBindAddresses().isEmpty()) {
        throw ZmqException.fatal();
      }
      if (!_target.frontendProps.getConnectAddresses().isEmpty()) {
        throw ZmqException.fatal();
      }
      if (!_target.backendProps.getConnectAddresses().isEmpty()) {
        throw ZmqException.fatal();
      }
    }
  }

  private ZmqSocketIdentityStorage socketIdentityStorage = new LruCache();

  //// CONSTRUCTORS

  private LruRouter() {
  }

  //// METHODS

  public static Builder builder() {
    return new Builder();
  }

  public void setSocketIdentityStorage(ZmqSocketIdentityStorage socketIdentityStorage) {
    this.socketIdentityStorage = socketIdentityStorage;
  }

  @Override
  public void init() {
    _frontend = ZmqChannel.builder()
                          .withCtx(ctx)
                          .ofROUTERType()
                          .withProps(frontendProps)
                          .build();

    _backend = ZmqChannel.builder()
                         .withCtx(ctx)
                         .ofROUTERType()
                         .withProps(backendProps)
                         .build();

    super.init();
  }

  @Override
  public void execute() {
    super.execute();

    // ==== handle backend traffic first ====
    if (_backend.canRecv()) {
      ZmqMessage message = _backend.recv();
      if (message == null) {
        LOG.error(".recv() failed on backend!");
        return;
      }

      ServiceHeaders headers = message.headersAs(ServiceHeaders.class);
      int numOfHops;
      try {
        numOfHops = headers.getNumOfHops();
      }
      catch (ZmqException e) {
        if (e.errorCode() == HEADER_IS_NOT_SET) {
          LOG.error("Ignoring message on backend: 'num_of_hops' header is missing!");
          return;
        }
        throw e;
      }

      if (headers.isMsgTypePing()) {
        // store worker' identities of every message coming from backend.
        socketIdentityStorage.store(message.identityFrames());
        return;
      }

      if (numOfHops < 0) {
        LOG.error("Ignoring message on backend: 'num_of_hops' is negative!");
        return;
      }

      ZmqFrames origIdentities = message.identityFrames();
      // mutate original backend identities (which in tuen consist of caller-identities + worker-identities)
      // and get worker' identities, in order to store them in the socket-identity-storage.
      int workerIdentityNum = origIdentities.size() - numOfHops;
      ZmqFrames workerIdentities = new ZmqFrames(workerIdentityNum);
      for (int i = 0; i < workerIdentityNum; i++) {
        workerIdentities.add(origIdentities.poll());
      }
      // store worker' identities of every message coming from backend.
      socketIdentityStorage.store(workerIdentities);

      boolean sent = _frontend.send(ZmqMessage.builder(message)
                                              .withIdentities(origIdentities)
                                              .build());
      if (!sent) {
        LOG.warn(".send() failed on frontend!");
      }
    }

    // ==== handle frontend traffic second ====
    if (_frontend.canRecv()) {
      if (socketIdentityStorage.size() <= 0) {
        return;
      }

      ZmqMessage message = _frontend.recv();
      if (message == null) {
        LOG.error(".recv() failed on frontend!");
        return;
      }

      ZmqFrames origIdentities = message.identityFrames();
      ZmqHeaders origHeaders = message.headers();
      ZmqMessage.Builder builder = ZmqMessage.builder();
      builder
          .withHeaders(new ServiceHeaders()
                           .copy(origHeaders)
                           .setNumOfHops(origIdentities.size()))
          .withPayload(message.payload());
      try {
        // prepend identities of the frontend message with backend identities obtained from storage.
        ZmqFrames targetIdentities = new ZmqFrames();
        targetIdentities.addAll(socketIdentityStorage.obtain(origIdentities));
        targetIdentities.addAll(origIdentities);
        builder.withIdentities(targetIdentities);
      }
      catch (ZmqException e) {
        ZmqException.ErrorCode errorCode = e.errorCode();
        if (errorCode == SOCKET_IDENTITY_STORAGE_IS_EMPTY || errorCode == SOCKET_IDENTITY_NOT_MATCHED) {
          LOG.info("Can't obtain routing_identity (err_code={}). Asking to retry ...", errorCode);
          boolean sentRetry = _frontend.send(ZmqMessage.builder(message)
                                                       .withHeaders(new ServiceHeaders()
                                                                        .copy(origHeaders)
                                                                        .setMsgTypeRetry())
                                                       .build());
          if (sentRetry) {
            LOG.info("Asked to retry.");
          }
          else {
            LOG.warn("Didn't send retry!");
          }
          return;
        }
        throw e;
      }

      boolean sent = _backend.send(builder.build());
      if (!sent) {
        LOG.warn(".send() failed on backend!");
      }
    }
  }
}
