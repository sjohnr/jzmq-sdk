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

package org.zeromq.messaging.device;

import org.zeromq.ZMQ;
import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqContext;
import org.zeromq.messaging.ZmqException;
import org.zeromq.support.HasInvariant;
import org.zeromq.support.ObjectBuilder;
import org.zeromq.support.thread.ZmqRunnableContext;

import java.util.ArrayList;
import java.util.List;

public abstract class ZmqAbstractRunnableContext implements ZmqRunnableContext, HasInvariant {

  private static final long DEFAULT_POLL_TIMEOUT = 1000;

  @SuppressWarnings("unchecked")
  public static abstract class Builder<B extends Builder, T extends ZmqAbstractRunnableContext>
      implements ObjectBuilder<T> {

    protected final T _target;

    protected Builder(T target) {
      this._target = target;
    }

    public final B withCtx(ZmqContext ctx) {
      _target.setCtx(ctx);
      return (B) this;
    }

    public final B withPollTimeout(long pollTimeout) {
      _target.setPollTimeout(pollTimeout);
      return (B) this;
    }

    @Override
    public final T build() {
      _target.checkInvariant();
      return _target;
    }
  }

  protected ZmqContext ctx;
  private long pollTimeout = DEFAULT_POLL_TIMEOUT;
  private List<ZmqChannel> channels = new ArrayList<ZmqChannel>();

  protected ZMQ.Poller _poller = new ZMQ.Poller(1);

  //// CONSTRUCTOR

  protected ZmqAbstractRunnableContext() {
  }

  //// METHODS

  public final void setCtx(ZmqContext ctx) {
    this.ctx = ctx;
  }

  public final void setPollTimeout(long pollTimeout) {
    this.pollTimeout = pollTimeout;
  }

  @Override
  public void checkInvariant() {
    if (ctx == null) {
      throw ZmqException.fatal();
    }
  }

  @Override
  public void execute() {
    _poller.poll(pollTimeout);
  }

  @Override
  public final void destroy() {
    for (ZmqChannel channel : channels) {
      channel.destroy();
    }
  }

  protected final void reg(ZmqChannel channel) {
    if (channel == null) {
      throw ZmqException.fatal();
    }
    channels.add(channel);
  }
}
