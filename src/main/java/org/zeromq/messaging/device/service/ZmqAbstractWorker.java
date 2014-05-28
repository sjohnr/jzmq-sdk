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

  protected static final int DEFAULT_PROC_LIMIT = 1000;

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

    public final B withPingStrategy(ZmqPingStrategy pingStrategy) {
      _target.setPingStrategy(pingStrategy);
      return (B) this;
    }

    public final B withMessageProcessor(ZmqMessageProcessor messageProcessor) {
      _target.setMessageProcessor(messageProcessor);
      return (B) this;
    }

    public final B withProcLimit(int procLimit) {
      _target.setProcLimit(procLimit);
      return (B) this;
    }
  }

  protected Props props;
  protected ZmqPingStrategy pingStrategy;
  protected ZmqMessageProcessor messageProcessor;
  protected int procLimit = DEFAULT_PROC_LIMIT;

  //// CONSTRUCTORS

  protected ZmqAbstractWorker() {
  }

  //// METHODS

  public final void setProps(Props props) {
    this.props = props;
  }

  public void setPingStrategy(ZmqPingStrategy pingStrategy) {
    this.pingStrategy = pingStrategy;
  }

  public final void setMessageProcessor(ZmqMessageProcessor messageProcessor) {
    this.messageProcessor = messageProcessor;
  }

  public final void setProcLimit(int procLimit) {
    this.procLimit = procLimit;
  }

  @Override
  public void checkInvariant() {
    super.checkInvariant();
    if (props == null) {
      throw ZmqException.fatal();
    }
    if (pingStrategy == null) {
      throw ZmqException.fatal();
    }
    if (messageProcessor == null) {
      throw ZmqException.fatal();
    }
  }

  @Override
  public void init() {
    channel(CHANNEL_ID_WORKER).watchRecv(_poller);
  }

  @Override
  public final void exec() {
    super.exec();

    ZmqChannel worker = channel(CHANNEL_ID_WORKER);

    // If no incoming traffic after polling -- send a PING.
    if (!worker.canRecv()) {
      pingStrategy.ping(worker);
      return;
    }

    for (int i = 0; i < procLimit; i++) {
      ZmqMessage request = worker.recvDontWait();
      if (request == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Request processed: " + i);
        }
        return;
      }
      ZmqMessage reply = null;
      try {
        reply = messageProcessor.process(request);
      }
      catch (Exception e) {
        LOG.error("Got issue at processing request: " + e, e);
      }
      if (reply != null) {
        boolean send = worker.send(reply);
        if (!send) {
          LOG.error("Can't send reply: " + reply);
        }
      }
    }
  }
}
