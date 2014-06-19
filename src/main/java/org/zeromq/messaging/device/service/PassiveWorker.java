package org.zeromq.messaging.device.service;

import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqException;

/**
 * Passive-worker device: (doesn't ping, just connects and awaits for incoming traffic; doesn't expose socket identity)
 * <pre>
 *   <-w(ROUTER) / no-ping / no identity to remote peer
 * </pre>
 */
public final class PassiveWorker extends ZmqAbstractWorker {

  public static class Builder extends ZmqAbstractWorker.Builder<Builder, PassiveWorker> {
    private Builder() {
      super(new PassiveWorker());
    }
  }

  //// CONSTRUCTORS

  private PassiveWorker() {
    pingStrategy = new DontPing();
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
    reg(CHANNEL_ID_WORKER, ZmqChannel.ROUTER(ctx).withProps(props).build());
    super.init();
  }
}
