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
import org.zeromq.messaging.device.ZmqAbstractDeviceContext;

import java.util.ArrayList;
import java.util.List;

public abstract class ZmqAbstractServiceDispatcher extends ZmqAbstractDeviceContext {

  @SuppressWarnings("unchecked")
  public static abstract class Builder<B extends Builder, T extends ZmqAbstractServiceDispatcher>
      extends ZmqAbstractDeviceContext.Builder<B, T> {

    protected Builder(T _target) {
      super(_target);
    }

    public final B withFrontendAddress(String address) {
      _target.frontendAddresses.add(address);
      return (B) this;
    }

    public final B withBackendAddress(String address) {
      _target.backendAddresses.add(address);
      return (B) this;
    }

    @Override
    public void checkInvariant() {
      super.checkInvariant();
      if (_target.frontendAddresses.isEmpty()) {
        throw ZmqException.fatal();
      }
      if (_target.backendAddresses.isEmpty()) {
        throw ZmqException.fatal();
      }
    }
  }

  protected List<String> frontendAddresses = new ArrayList<String>();
  protected List<String> backendAddresses = new ArrayList<String>();

  protected ZmqChannel _frontend;
  protected ZmqChannel _backend;

  //// CONSTRUCTORS

  protected ZmqAbstractServiceDispatcher() {
  }

  //// METHODS

  public final void setFrontendAddresses(List<String> frontendAddresses) {
    this.frontendAddresses = frontendAddresses;
  }

  public final void setBackendAddresses(List<String> backendAddresses) {
    this.backendAddresses = backendAddresses;
  }

  @Override
  public void init() {
    if (_frontend == null) {
      throw ZmqException.fatal();
    }
    if (_backend == null) {
      throw ZmqException.fatal();
    }
    _poller = zmqContext.newPoller(2);
    _frontend.watchRecv(_poller);
    _backend.watchRecv(_poller);
  }

  @Override
  public final void destroy() {
    if (_frontend != null) {
      _frontend.destroy();
    }
    if (_backend != null) {
      _backend.destroy();
    }
  }
}
