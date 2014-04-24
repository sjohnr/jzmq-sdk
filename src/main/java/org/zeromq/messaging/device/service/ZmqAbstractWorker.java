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
import org.zeromq.messaging.device.ZmqAbstractActor;

public abstract class ZmqAbstractWorker extends ZmqAbstractActor {

  protected static final Logger LOG = LoggerFactory.getLogger(ZmqAbstractWorker.class);

  protected static final String CHANNEL_ID_WORKER = "worker";

  @SuppressWarnings("unchecked")
  public static abstract class Builder<B extends Builder, T extends ZmqAbstractWorker>
      extends ZmqAbstractActor.Builder<B, T> {

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
  }

  protected Props props;
  protected ZmqMessageProcessor messageProcessor;

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
  public void checkInvariant() {
    super.checkInvariant();
    if (props == null) {
      throw ZmqException.fatal();
    }
    if (messageProcessor == null) {
      throw ZmqException.fatal();
    }
  }

  @Override
  public void init() {
    channel(CHANNEL_ID_WORKER).watchRecv(_poller);
    _pingStrategyForLogging = _pingStrategy.getClass().getSimpleName();
  }

  @Override
  public final void exec() {
    super.exec();

    ZmqChannel workerChannel = channel(CHANNEL_ID_WORKER);

    if (!workerChannel.canRecv()) {
      LOG.debug("No incoming requests. Delegating to {}.", _pingStrategyForLogging);
      _pingStrategy.ping(workerChannel);
      return;
    }

    // receive incoming traffic.
    ZmqMessage request = workerChannel.recv();
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
      _pingStrategy.ping(workerChannel);
      return;
    }
    // if reply is not null -- send it / otherwise -- ping.
    if (reply != null) {
      if (!workerChannel.send(reply)) {
        LOG.warn(".send() failed!");
      }
    }
    else {
      _pingStrategy.ping(workerChannel);
    }
  }
}
