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

import org.zeromq.messaging.ZmqChannelFactory;

/**
 * Fair device:
 * <pre>
 *   [:f(ROUTER) / :b(DEALER)]
 * </pre>
 */
public final class FairRouter extends ZmqAbstractFairServiceDispatcher {

  public static class Builder extends ZmqAbstractServiceDispatcher.Builder<Builder, FairRouter> {
    private Builder() {
      super(new FairRouter());
    }
  }

  //// CONSTRUCTORS

  private FairRouter() {
  }

  //// METHODS

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public void init() {
    _frontendFactory = ZmqChannelFactory.builder()
                                        .ofROUTERType()
                                        .withZmqContext(zmqContext)
                                        .withEventListeners(frontendEventListeners)
                                        .withBindAddresses(frontendAddresses)
                                        .build();
    _backendFactory = ZmqChannelFactory.builder()
                                       .ofDEALERType()
                                       .withZmqContext(zmqContext)
                                       .withEventListeners(backendEventListeners)
                                       .withBindAddresses(backendAddresses)
                                       .build();

    _frontend = _frontendFactory.newChannel();
    _backend = _backendFactory.newChannel();

    super.init();
  }
}
