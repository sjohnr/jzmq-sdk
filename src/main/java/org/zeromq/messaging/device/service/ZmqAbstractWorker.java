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

    public final B withPingStrategy(ZmqPingStrategy pingStrategy) {
      _target.setPingStrategy(pingStrategy);
      return (B) this;
    }

    public final B withMessageProcessor(ZmqMessageProcessor messageProcessor) {
      _target.setMessageProcessor(messageProcessor);
      return (B) this;
    }
  }

  protected Props props;
  protected ZmqPingStrategy pingStrategy;
  protected ZmqMessageProcessor messageProcessor;

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

    if (!worker.canRecv()) {
      pingStrategy.ping(worker);
      return;
    }

    for (; ; ) {
      ZmqMessage request = worker.recvDontWait();
      if (request == null) {
        return;
      }
      ZmqMessage reply = null;
      try {
        reply = messageProcessor.process(request);
      }
      catch (Exception e) {
        LOG.error("Got issue at processing request: " + request, e);
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
