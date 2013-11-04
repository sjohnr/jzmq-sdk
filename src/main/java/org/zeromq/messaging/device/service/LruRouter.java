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
import org.zeromq.messaging.ZmqChannelFactory;
import org.zeromq.messaging.ZmqException;
import org.zeromq.messaging.ZmqFrames;
import org.zeromq.messaging.ZmqMessage;
import org.zeromq.messaging.device.ZmqSocketIdentityStorage;

/**
 * LRU device:
 * <pre>
 *   [:f(ROUTER) / :b(ROUTER)]
 * </pre>
 */
public final class LruRouter extends ZmqAbstractServiceDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(LruRouter.class);

  public static final class Builder extends ZmqAbstractServiceDispatcher.Builder<Builder, LruRouter> {

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
    _frontendFactory = ZmqChannelFactory.builder()
                                        .withZmqContext(zmqContext)
                                        .withEventListeners(frontendEventListeners)
                                        .withBindAddresses(frontendAddresses)
                                        .ofROUTERType()
                                        .build();
    _backendFactory = ZmqChannelFactory.builder()
                                       .withZmqContext(zmqContext)
                                       .withEventListeners(backendEventListeners)
                                       .withBindAddresses(backendAddresses)
                                       .ofROUTERType()
                                       .build();

    _frontend = _frontendFactory.newChannel();
    _backend = _backendFactory.newChannel();

    super.init();
  }

  @Override
  public void exec() {
    // handle backend traffic first.
    if (_backend.hasInput()) {
      ZmqMessage backendMessage = _backend.recv();
      ServiceHeaders headers = backendMessage.headersAs(ServiceHeaders.class);
      try {
        ZmqFrames backendIdentities = backendMessage.identities();
        int numOfHops = headers.getNumOfHops();
        if (numOfHops > 0) {
          // mutate original backend identities and get plain backend identities.
          int plainBackendIdentitiesNum = backendIdentities.size() - numOfHops;
          ZmqFrames plainBackendIdentities = new ZmqFrames(plainBackendIdentitiesNum);
          for (int i = 0; i < plainBackendIdentitiesNum; i++) {
            plainBackendIdentities.add(backendIdentities.poll());
          }
          backendIdentities = plainBackendIdentities;
        }
        // store identity for any message coming from backend.
        socketIdentityStorage.store(backendIdentities);
        // filter PING message.
        if (headers.isMsgTypePing()) {
          return;
        }
        // send backend message via frontend socket next down on the chain onto the original receipient.
        _frontend.send(backendMessage);
      }
      catch (ZmqException e) {
        if (e.errorCode() == ZmqException.ErrorCode.HEADER_IS_NOT_SET) {
          LOG.error("LRU will ignore message on backend.");
          return;
        }
        throw e;
      }
    }
    // handle frontend traffic second.
    if (_frontend.hasInput()) {
      if (socketIdentityStorage.size() <= 0) {
        return;
      }

      ZmqMessage frontendMessage = _frontend.recv();
      ZmqFrames frontendIdentities = frontendMessage.identities();
      // set number_of_hops using frontend identities total size.
      ZmqMessage.Builder builder = ZmqMessage.builder();
      builder
          .withHeaders(frontendMessage.headers())
          .withHeaders(new ServiceHeaders().setNumOfHops(frontendIdentities.size()))
          .withPayload(frontendMessage.payload());
      try {
        // prepend identities of the frontend message with backend identities obtained from storage.
        builder
            .withIdentities(socketIdentityStorage.obtain(frontendIdentities))
            .withIdentities(frontendIdentities);
      }
      catch (ZmqException e) {
        if (e.errorCode() == ZmqException.ErrorCode.SOCKET_IDENTITY_STORAGE_IS_EMPTY ||
            e.errorCode() == ZmqException.ErrorCode.SOCKET_IDENTITY_NOT_MATCHED) {
          LOG.warn("Obtaining routing_identity failed (err_code={}). Asking to TRY_AGAIN ...", e.errorCode());
          _frontend.send(ZmqMessage.builder()
                                   .withIdentities(frontendIdentities)
                                   .withHeaders(frontendMessage.headers())
                                   .withHeaders(new ServiceHeaders().setMsgTypeTryAgain())
                                   .withPayload(frontendMessage.payload())
                                   .build()
          );
          LOG.warn("Asked to TRY_AGAIN.");
          return;
        }
        // if this is not socket-identity-storage related exception -- then re-throw.
        throw e;
      }
      _backend.send(builder.build());
    }
  }
}
