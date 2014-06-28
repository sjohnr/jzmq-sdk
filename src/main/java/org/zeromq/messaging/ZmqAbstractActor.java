package org.zeromq.messaging;

import org.zeromq.ZMQ;
import org.zeromq.support.HasInvariant;
import org.zeromq.support.ObjectBuilder;
import org.zeromq.support.thread.ZmqActor;

import java.util.HashMap;
import java.util.Map;

public abstract class ZmqAbstractActor implements ZmqActor, HasInvariant {

  private static final long DEFAULT_POLL_TIMEOUT = 1000;

  @SuppressWarnings("unchecked")
  public static abstract class Builder<B extends Builder, T extends ZmqAbstractActor>
      implements ObjectBuilder<T> {

    protected final T _target;

    protected Builder(T target) {
      this._target = target;
    }

    public final B withCtx(ZmqContext ctx) {
      _target.setCtx(ctx);
      return (B) this;
    }

    public final B withPollTimeout(long pollTimeout) {
      _target.setPollTimeout(pollTimeout);
      return (B) this;
    }

    @Override
    public final T build() {
      _target.checkInvariant();
      return _target;
    }
  }

  protected ZmqContext ctx;
  private long pollTimeout = DEFAULT_POLL_TIMEOUT;

  protected ZMQ.Poller _poller = new ZMQ.Poller(1);
  protected Map<String, ZmqChannel> _channels = new HashMap<String, ZmqChannel>();

  //// CONSTRUCTOR

  protected ZmqAbstractActor() {
  }

  //// METHODS

  public final void setCtx(ZmqContext ctx) {
    this.ctx = ctx;
  }

  public final void setPollTimeout(long pollTimeout) {
    this.pollTimeout = pollTimeout;
  }

  @Override
  public void checkInvariant() {
    if (ctx == null) {
      throw ZmqException.fatal();
    }
  }

  @Override
  public void exec() throws Exception {
    _poller.poll(pollTimeout);
  }

  @Override
  public final void destroy() {
    for (ZmqChannel channel : _channels.values()) {
      channel.destroy();
    }
    _channels.clear();
  }

  protected final ZmqChannel register(String id, ZmqChannel channel) {
    if (channel == null) {
      throw ZmqException.fatal();
    }
    _channels.put(id, channel);
    return channel;
  }

  protected final ZmqChannel channel(String id) {
    ZmqChannel channel = _channels.get(id);
    if (channel == null) {
      throw ZmqException.fatal();
    }
    return channel;
  }
}
