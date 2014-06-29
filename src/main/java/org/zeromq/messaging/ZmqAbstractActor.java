package org.zeromq.messaging;

import org.zeromq.ZMQ;
import org.zeromq.support.HasInvariant;
import org.zeromq.support.ObjectBuilder;
import org.zeromq.support.thread.ZmqActor;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

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
    checkArgument(ctx != null);
  }

  @Override
  public final void destroy() {
    for (ZmqChannel channel : _channels.values()) {
      channel.destroy();
    }
    _channels.clear();
  }

  protected final void poll() {
    _poller.poll(pollTimeout);
  }

  protected final ZmqChannel put(String id, ZmqChannel channel) {
    checkArgument(id != null && !id.trim().isEmpty(), "Wrong channelId=" + id);
    checkArgument(channel != null);
    _channels.put(id, channel);
    return channel;
  }

  protected final ZmqChannel channel(String id) {
    return _channels.get(id);
  }
}
