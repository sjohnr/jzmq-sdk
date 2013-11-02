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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqChannelFactory;
import org.zeromq.messaging.ZmqException;
import org.zeromq.messaging.ZmqMessage;
import org.zeromq.messaging.device.ZmqAbstractDeviceContext;

import java.util.ArrayList;
import java.util.List;

public abstract class ZmqAbstractWorker extends ZmqAbstractDeviceContext {

  private static final Logger LOG = LoggerFactory.getLogger(ZmqAbstractWorker.class);

  @SuppressWarnings("unchecked")
  public static abstract class Builder<B extends Builder, T extends ZmqAbstractWorker>
      extends ZmqAbstractDeviceContext.Builder<B, T> {

    protected Builder(T _target) {
      super(_target);
    }

    public final B withEventListener(Object el) {
      _target.eventListeners.add(el);
      return (B) this;
    }

    public final B withEventListeners(Iterable eventListeners) {
      for (Object el : eventListeners) {
        withEventListener(el);
      }
      return (B) this;
    }

    public final B withMessageProcessor(ZmqMessageProcessor messageProcessor) {
      _target.setMessageProcessor(messageProcessor);
      return (B) this;
    }

    public final B withConnectAddress(String address) {
      _target.connectAddresses.add(address);
      return (B) this;
    }

    public final B withConnectAddresses(Iterable<String> addresses) {
      for (String address : addresses) {
        withConnectAddress(address);
      }
      return (B) this;
    }

    public final B withConnectAddresses(String[] addresses) {
      for (String address : addresses) {
        withConnectAddress(address);
      }
      return (B) this;
    }

    public final B withBindAddress(String address) {
      _target.bindAddresses.add(address);
      return (B) this;
    }

    public final B withBindAddresses(Iterable<String> addresses) {
      for (String address : addresses) {
        withBindAddress(address);
      }
      return (B) this;
    }

    public final B withBindAddresses(String[] addresses) {
      for (String address : addresses) {
        withBindAddress(address);
      }
      return (B) this;
    }

    public void checkInvariant() {
      super.checkInvariant();
      if (_target.messageProcessor == null) {
        throw ZmqException.fatal();
      }
    }
  }

  protected ZmqMessageProcessor messageProcessor;
  protected List<String> connectAddresses = new ArrayList<String>();
  protected List<String> bindAddresses = new ArrayList<String>();
  protected List<Object> eventListeners = new ArrayList<Object>();

  protected ZmqChannelFactory _channelFactory;
  protected ZmqChannel _channel;
  protected ZmqPingStrategy _pingStrategy;
  protected String _pingStrategyForLogging;

  //// CONSTRUCTORS

  protected ZmqAbstractWorker() {
  }

  //// METHODS

  public final void setEventListeners(List<Object> eventListeners) {
    this.eventListeners = eventListeners;
  }

  public final void setMessageProcessor(ZmqMessageProcessor messageProcessor) {
    this.messageProcessor = messageProcessor;
  }

  public final void setConnectAddresses(List<String> connectAddresses) {
    this.connectAddresses = connectAddresses;
  }

  public final void setBindAddresses(List<String> bindAddresses) {
    this.bindAddresses = bindAddresses;
  }

  @Override
  public void init() {
    _poller = zmqContext.newPoller(1);
    _channel = _channelFactory.newChannel();
    _channel.register(_poller);
    _pingStrategyForLogging = _pingStrategy.getClass().getSimpleName();
  }

  @Override
  public final void exec() {
    if (!_channel.hasInput()) {
      LOG.debug("No incoming requests. Delegating to {}.", _pingStrategyForLogging);
      _pingStrategy.ping(_channel);
      return;
    }
    // receive incoming traffic.
    ZmqMessage request = _channel.recv();
    ZmqMessage reply;
    try {
      reply = messageProcessor.process(request);
    }
    catch (Exception e) {
      LOG.error("!!! Failed at processing request. Delegating to {} anyway.", _pingStrategyForLogging);
      // request processing failed -- still need to ping.
      _pingStrategy.ping(_channel);
      return;
    }
    // if reply is not null -- send it / otherwise -- ping.
    if (reply != null) {
      _channel.send(reply);
    }
    else {
      _pingStrategy.ping(_channel);
    }
  }

  @Override
  public final void destroy() {
    if (_channel != null) {
      _channel.destroy();
    }
  }
}
