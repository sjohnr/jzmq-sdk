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

import org.zeromq.messaging.Props;
import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqException;

public abstract class ZmqAbstractProxy extends ZmqAbstractRunnableContext {

  @SuppressWarnings("unchecked")
  public static abstract class Builder<B extends Builder, T extends ZmqAbstractProxy>
      extends ZmqAbstractRunnableContext.Builder<B, T> {

    protected Builder(T target) {
      super(target);
    }

    public final B withFrontendProps(Props props) {
      _target.frontendProps = props;
      return (B) this;
    }

    public final B withBackendProps(Props props) {
      _target.backendProps = props;
      return (B) this;
    }

    @Override
    public void checkInvariant() {
      super.checkInvariant();
      if (_target.frontendProps == null) {
        throw ZmqException.fatal();
      }
      if (_target.backendProps == null) {
        throw ZmqException.fatal();
      }
    }
  }

  protected Props frontendProps;
  protected Props backendProps;

  protected ZmqChannel _frontend;
  protected ZmqChannel _backend;

  //// CONSTRUCTORS

  protected ZmqAbstractProxy() {
  }

  //// METHODS

  public final void setFrontendProps(Props frontendProps) {
    this.frontendProps = frontendProps;
  }

  public final void setBackendProps(Props backendProps) {
    this.backendProps = backendProps;
  }

  @Override
  public void init() {
    if (_frontend == null) {
      throw ZmqException.fatal();
    }
    if (_backend == null) {
      throw ZmqException.fatal();
    }
    _frontend.watchRecv(_poller);
    _backend.watchRecv(_poller);
  }
}
