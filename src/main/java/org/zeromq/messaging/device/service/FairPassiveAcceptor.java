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

import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqException;
import org.zeromq.messaging.device.ZmqAbstractProxy;

/**
 * Fair device:
 * <pre>
 *   [<-f(ROUTER) / :b(DEALER)]
 * </pre>
 */
public final class FairPassiveAcceptor extends ZmqAbstractFairService {

  public static class Builder extends ZmqAbstractProxy.Builder<Builder, FairPassiveAcceptor> {

    private Builder() {
      super(new FairPassiveAcceptor());
    }

    @Override
    public void checkInvariant() {
      super.checkInvariant();
      if (!_target.frontendProps.getBindAddresses().isEmpty()) {
        throw ZmqException.fatal();
      }
      if (_target.backendProps.getBindAddresses().isEmpty()) {
        throw ZmqException.fatal();
      }
      if (_target.frontendProps.getConnectAddresses().isEmpty()) {
        throw ZmqException.fatal();
      }
      if (!_target.backendProps.getConnectAddresses().isEmpty()) {
        throw ZmqException.fatal();
      }
    }
  }

  //// CONSTRUCTORS

  private FairPassiveAcceptor() {
  }

  //// METHODS

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public void init() {
    _frontend = ZmqChannel.builder()
                          .withZmqContext(zmqContext)
                          .ofROUTERType()
                          .withProps(frontendProps)
                          .build();

    _backend = ZmqChannel.builder()
                         .withZmqContext(zmqContext)
                         .ofDEALERType()
                         .withProps(backendProps)
                         .build();

    super.init();
  }
}
