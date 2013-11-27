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

/**
 * Fair device:
 * <pre>
 *   [<-f(DEALER) / :b(ROUTER)]
 * </pre>
 */
public final class FairActiveAcceptor extends ZmqAbstractFairServiceDispatcher {

  public static class Builder extends ZmqAbstractServiceDispatcher.Builder<Builder, FairActiveAcceptor> {
    private Builder() {
      super(new FairActiveAcceptor());
    }
  }

  //// CONSTRUCTORS

  private FairActiveAcceptor() {
  }

  //// METHODS

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public void init() {
    _frontend = ZmqChannel.builder()
                          .ofDEALERType()
                          .withZmqContext(zmqContext)
                          .withConnectAddresses(frontendAddresses)
                          .build();
    _backend = ZmqChannel.builder()
                         .ofROUTERType()
                         .withZmqContext(zmqContext)
                         .withBindAddresses(backendAddresses)
                         .build();

    super.init();
  }
}
