package org.zeromq.messaging.device.service;

import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqException;

/**
 * Modest-worker device: (does ping only once, exposes socket identity via DEALER)
 * <pre>
 *   <-w(DEALER) / with-ping-only-once / exposes identity to remote peer
 * </pre>
 */
public final class ModestWorker extends ZmqAbstractWorker {

  public static class Builder extends ZmqAbstractWorker.Builder<Builder, ModestWorker> {
    private Builder() {
      super(new ModestWorker());
    }
  }

  //// CONSTRUCTORS

  private ModestWorker() {
    pingStrategy = new DoPingOnce();
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
