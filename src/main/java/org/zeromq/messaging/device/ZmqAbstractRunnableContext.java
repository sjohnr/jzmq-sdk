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
import org.zeromq.messaging.ZmqContext;
import org.zeromq.messaging.ZmqException;
import org.zeromq.support.ObjectBuilder;
import org.zeromq.support.thread.ZmqRunnableContext;

public abstract class ZmqAbstractRunnableContext implements ZmqRunnableContext {

  private static final long DEFAULT_POLL_TIMEOUT = 1000;

  @SuppressWarnings("unchecked")
  public static abstract class Builder<B extends Builder, T extends ZmqAbstractRunnableContext>
      implements ObjectBuilder<T> {

    protected final T _target;

    protected Builder(T _target) {
      this._target = _target;
    }

    public final B withZmqContext(ZmqContext zmqContext) {
      _target.setZmqContext(zmqContext);
      return (B) this;
    }

    public final B withPollTimeout(long pollTimeout) {
      _target.setPollTimeout(pollTimeout);
      return (B) this;
    }

    @Override
    public void checkInvariant() {
      if (_target.zmqContext == null) {
        throw ZmqException.fatal();
      }
    }

    @Override
    public final T build() {
      checkInvariant();
      return _target;
    }
  }

  protected ZmqContext zmqContext;
  protected long pollTimeout = DEFAULT_POLL_TIMEOUT;

  protected ZMQ.Poller _poller;

  //// CONSTRUCTOR

  protected ZmqAbstractRunnableContext() {
  }

  //// METHODS

  public final void setZmqContext(ZmqContext zmqContext) {
    this.zmqContext = zmqContext;
  }

  public final void setPollTimeout(long pollTimeout) {
    this.pollTimeout = pollTimeout;
  }

  @Override
  public final void block() {
    if (_poller == null) {
      throw ZmqException.fatal();
    }
    _poller.poll(pollTimeout);
  }
}
