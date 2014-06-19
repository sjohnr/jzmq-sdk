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
