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
 * Active-worker device: (does ping, exposes socket identity via DEALER)
 * <pre>
 *   <-w(DEALER) / with-ping / exposes identity to remote peer
 * </pre>
 */
public final class ActiveWorker extends ZmqAbstractWorker {

  public static class Builder extends ZmqAbstractWorker.Builder<Builder, ActiveWorker> {
    private Builder() {
      super(new ActiveWorker());
    }
  }

  //// CONSTRUCTORS

  private ActiveWorker() {
    pingStrategy = new DoPing();
  }

  //// METHODS

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public void setPingStrategy(ZmqPingStrategy pingStrategy) {
    throw new UnsupportedOperationException();
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
    reg(CHANNEL_ID_WORKER, ZmqChannel.DEALER(ctx).withProps(props).build());
    super.init();
  }
}
