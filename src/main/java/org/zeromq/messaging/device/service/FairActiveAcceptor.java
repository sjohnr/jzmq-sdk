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
 *   [<-f(DEALER) / :b(ROUTER)]
 * </pre>
 */
public final class FairActiveAcceptor extends ZmqAbstractFairService {

  public static class Builder extends ZmqAbstractProxy.Builder<Builder, FairActiveAcceptor> {
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
  public void checkInvariant() {
    super.checkInvariant();
    if (frontendProps.bindAddr() != null) {
      throw ZmqException.fatal();
    }
    if (backendProps.bindAddr() == null) {
      throw ZmqException.fatal();
    }
    if (frontendProps.connectAddr() == null) {
      throw ZmqException.fatal();
    }
    if (backendProps.connectAddr() != null) {
      throw ZmqException.fatal();
    }
  }

  @Override
  public void init() {
    checkInvariant();

    reg(_frontend = ZmqChannel.DEALER(ctx).withProps(frontendProps).build());
    reg(_backend = ZmqChannel.ROUTER(ctx).withProps(backendProps).build());

    super.init();
  }
}
