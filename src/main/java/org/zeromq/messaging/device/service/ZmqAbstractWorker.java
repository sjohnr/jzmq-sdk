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
import org.zeromq.messaging.Props;
import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqException;
import org.zeromq.messaging.ZmqMessage;
import org.zeromq.messaging.device.ZmqAbstractRunnableContext;

public abstract class ZmqAbstractWorker extends ZmqAbstractRunnableContext {

  protected static final Logger LOG = LoggerFactory.getLogger(ZmqAbstractWorker.class);

  @SuppressWarnings("unchecked")
  public static abstract class Builder<B extends Builder, T extends ZmqAbstractWorker>
      extends ZmqAbstractRunnableContext.Builder<B, T> {

    protected Builder(T target) {
      super(target);
    }

    public final B withProps(Props props) {
      _target.setProps(props);
      return (B) this;
    }

    public final B withMessageProcessor(ZmqMessageProcessor messageProcessor) {
      _target.setMessageProcessor(messageProcessor);
      return (B) this;
    }

    public void checkInvariant() {
      super.checkInvariant();
      if (_target.props == null) {
        throw ZmqException.fatal();
      }
      if (_target.messageProcessor == null) {
        throw ZmqException.fatal();
      }
    }
  }

  protected Props props;
  protected ZmqMessageProcessor messageProcessor;

  protected ZmqChannel _channel;
  protected ZmqPingStrategy _pingStrategy;
  protected String _pingStrategyForLogging;

  //// CONSTRUCTORS

  protected ZmqAbstractWorker() {
  }

  //// METHODS


  public final void setProps(Props props) {
    this.props = props;
  }

  public final void setMessageProcessor(ZmqMessageProcessor messageProcessor) {
    this.messageProcessor = messageProcessor;
  }

  @Override
  public void init() {
    if (_channel == null) {
      throw ZmqException.fatal();
    }
    _channel.watchRecv(_poller);
    _pingStrategyForLogging = _pingStrategy.getClass().getSimpleName();
  }

  @Override
  public final void execute() {
    super.execute();

    if (!_channel.canRecv()) {
      LOG.debug("No incoming requests. Delegating to {}.", _pingStrategyForLogging);
      _pingStrategy.ping(_channel);
      return;
    }

    // receive incoming traffic.
    ZmqMessage request = _channel.recv();
    if (request == null) {
      LOG.error(".recv() failed!");
      return;
    }

    ZmqMessage reply;
    try {
      reply = messageProcessor.process(request);
    }
    catch (Exception e) {
      LOG.error("Failed at processing request. Delegating to {} anyway.", _pingStrategyForLogging);
      // request processing failed -- still need to ping.
      _pingStrategy.ping(_channel);
      return;
    }
    // if reply is not null -- send it / otherwise -- ping.
    if (reply != null) {
      if (!_channel.send(reply)) {
        LOG.warn(".send() failed!");
      }
    }
    else {
      _pingStrategy.ping(_channel);
    }
  }
}
