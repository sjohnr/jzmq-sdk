package org.zeromq.messaging.device.service;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.messaging.Props;
import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqContext;
import org.zeromq.messaging.ZmqException;
import org.zeromq.messaging.ZmqMessage;
import org.zeromq.support.HasDestroy;
import org.zeromq.support.HasInvariant;
import org.zeromq.support.ObjectBuilder;

import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public final class ZmqCaller implements HasDestroy {

  private static final long DEFAULT_RETRY_TIMEOUT = 3000; // timeout before give up and returning null.

  private static final Logger LOG = LoggerFactory.getLogger(ZmqCaller.class);

  /** State indicating that client can {@code send}. */
  private static final int B_SEND = 0;
  /** State indicating that client can {@code recv} (see other derived {@code recv} states further). */
  private static final int B_RECV = 1;
  /** Derived {@code recv} state indicating that client can {@code recv_reply}. */
  private static final int B_RECV$REPLY = 2;
  /** Derived {@code recv} state indicating that client can {@code retry_send}. */
  private static final int B_RECV$RETRY_SEND = 3;
  /** Derived {@code recv} state indicating that client can {@code validate_received_reply}. */
  private static final int B_RECV$VALIDATE_REPLY = 4;

  public static final class Builder implements ObjectBuilder<ZmqCaller>, HasInvariant {

    private final ZmqCaller _target = new ZmqCaller();

    private Builder(ZmqContext ctx) {
      _target.ctx = ctx;
    }

    public Builder withChannelProps(Props channelProps) {
      _target.channelProps = channelProps;
      return this;
    }

    public Builder withRetryTimeout(long retryTimeout) {
      _target.retryTimeout = retryTimeout;
      return this;
    }

    public Builder withCidProvider(ObjectBuilder<Long> cidProvider) {
      _target.cidProvider = cidProvider;
      return this;
    }

    @Override
    public ZmqCaller build() {
      checkInvariant();

      _target.initFsm();
      _target._channel = ZmqChannel.DEALER(_target.ctx)
                                   .withProps(_target.channelProps)
                                   .build();

      return _target;
    }

    @Override
    public void checkInvariant() {
      if (_target.ctx == null) {
        throw ZmqException.fatal();
      }
      if (_target.channelProps == null) {
        throw ZmqException.fatal();
      }
      if (_target.cidProvider == null) {
        throw ZmqException.fatal();
      }
    }
  }

  private ZmqContext ctx;
  private Props channelProps;
  private long retryTimeout = DEFAULT_RETRY_TIMEOUT;
  private ObjectBuilder<Long> cidProvider = new ObjectBuilder<Long>() {
    @Override
    public Long build() {
      return UUID.randomUUID().getMostSignificantBits(); // default UUID :|
    }
  };

  private ZmqChannel _channel;
  private BitSet _fsm = new BitSet(5);
  private Set<Long> _cids = new HashSet<Long>();

  private ZmqCaller() {
  }

  public static Builder builder(ZmqContext ctx) {
    return new Builder(ctx);
  }

  public long send(ZmqMessage message) {
    boolean wasRecv = setSend();

    if (wasRecv) {
      // clear any saved instance_state which was bound to previous state B_SEND.
      _cids.clear();
    }

    long cid = cidProvider.build();
    Preconditions.checkState(_cids.add(cid), "CidProvider generates not unique correlation_id={}.", cid);

    message = ZmqMessage.builder(message)
                        .withHeaders(new ServiceHeaders()
                                         .copy(message.headers())
                                         .setCid(cid))
                        .build();

    _channel.send(message);
    return cid;
  }

  public ZmqMessage recv() {
    setRecv();
    setRecvReply(); // initially FSM is set to "receive reply".

    Stopwatch timer = new Stopwatch().start();
    ZmqMessage message = null;
    ServiceHeaders headers = null;
    for (; ; ) {
      // receive reply.
      if (isRecvReply()) {
        message = _channel.recv();
        if (message == null) {
          return null;
        }
        headers = message.headersAs(ServiceHeaders.class);
        setRecvValidateReply();
      }
      // validate reply: check corresponding headers.
      if (isRecvValidateReply()) {
        // check correlation.
        long cid = headers.getCid();
        if (!_cids.contains(cid)) {
          LOG.warn("Unrecognized correlation_id={}.", cid);
          setRecvReply(); // recv reply again.
          continue;
        }
        // check headers.
        if (headers.isMsgTypeNotSet()) {
          return message;
        }
        else if (headers.isMsgTypeRetry()) {
          setRecvRetrySend();
        }
        else {
          String msgType = headers.getHeaderOrException(ServiceHeaders.HEADER_MSG_TYPE);
          LOG.error("!!! Unsupported msg_type={} detected.", msgType);
          throw ZmqException.wrongMessage();
        }
      }
      // got retry command: check the retryTimeout and a timer.
      if (isRecvRetrySend()) {
        if (retryTimeout == 0) {
          return null;
        }
        if (retryTimeout > 0 && retryTimeout - timer.elapsedMillis() <= 0) {
          LOG.warn("Can't retry: timeout({} ms) exceeded. Returning null.", retryTimeout);
          return null;
        }
        boolean sent = _channel.send(ZmqMessage.builder(message)
                                               .withHeaders(new ServiceHeaders()
                                                                .copy(message.headers())
                                                                .setMsgTypeNotSet())
                                               .build());
        if (!sent) {
          return null;
        }
        setRecvReply();
      }
    }
  }

  @Override
  public void destroy() {
    if (_channel != null) {
      _channel.destroy();
    }
  }

  /** FSM init function. Turns on only two primary states: {@code send} and {@code recv}. */
  private void initFsm() {
    _fsm.set(B_SEND);
    _fsm.set(B_RECV);
  }

  /** Turns on {@code send} state and clears all rest. Return true if prev state was {@code recv}. */
  private boolean setSend() {
    _fsm.set(B_SEND);
    boolean wasRecv = _fsm.get(B_RECV);
    _fsm.clear(B_RECV);
    _fsm.clear(B_RECV$REPLY);
    _fsm.clear(B_RECV$RETRY_SEND);
    _fsm.clear(B_RECV$VALIDATE_REPLY);
    return wasRecv;
  }

  /** Turns on {@code recv} state and clears all rest. Return true if prev state was {@code send}. */
  private boolean setRecv() {
    _fsm.set(B_RECV);
    boolean wasSend = _fsm.get(B_SEND);
    _fsm.clear(B_SEND);
    _fsm.clear(B_RECV$REPLY);
    _fsm.clear(B_RECV$RETRY_SEND);
    _fsm.clear(B_RECV$VALIDATE_REPLY);
    return wasSend;
  }

  /** Turns on {@code retry_send} state and clears all rest derived {@code recv} states. */
  private void setRecvRetrySend() {
    _fsm.set(B_RECV$RETRY_SEND);
    _fsm.clear(B_RECV$REPLY);
    _fsm.clear(B_RECV$VALIDATE_REPLY);
  }

  private boolean isRecvRetrySend() {
    return _fsm.get(B_RECV$RETRY_SEND);
  }

  /** Turns on {@code recv_reply} state and clears all rest derived {@code recv} states. */
  private void setRecvReply() {
    _fsm.set(B_RECV$REPLY);
    _fsm.clear(B_RECV$RETRY_SEND);
    _fsm.clear(B_RECV$VALIDATE_REPLY);
  }

  private boolean isRecvReply() {
    return _fsm.get(B_RECV$REPLY);
  }

  /** Turns on {@code validate_received_reply} state and clears all rest derived {@code recv} states. */
  private void setRecvValidateReply() {
    _fsm.set(B_RECV$VALIDATE_REPLY);
    _fsm.clear(B_RECV$REPLY);
    _fsm.clear(B_RECV$RETRY_SEND);
  }

  private boolean isRecvValidateReply() {
    return _fsm.get(B_RECV$VALIDATE_REPLY);
  }
}
