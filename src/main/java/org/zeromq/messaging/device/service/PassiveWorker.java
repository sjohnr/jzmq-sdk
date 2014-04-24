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

/**
 * Passive-worker device: (doesn't ping, just connects and awaits for incoming traffic; doesn't expose socket identity)
 * <pre>
 *   <-w(ROUTER) / no-ping / no identity to remote peer
 * </pre>
 */
public final class PassiveWorker extends ZmqAbstractWorker {

  public static class Builder extends ZmqAbstractWorker.Builder<Builder, PassiveWorker> {
    private Builder() {
      super(new PassiveWorker());
    }
  }

  //// CONSTRUCTORS

  private PassiveWorker() {
  }

  //// METHODS

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public void checkInvariant() {
    super.checkInvariant();
    if (props.connectAddr().isEmpty()) {
      throw ZmqException.fatal();
    }
    if (!props.bindAddr().isEmpty()) {
      throw ZmqException.fatal();
    }
  }

  @Override
  public void init() {
    checkInvariant();

    _pingStrategy = new DontPing();
    reg(_channel = ZmqChannel.ROUTER(ctx).withProps(props).build());

    super.init();
  }
}