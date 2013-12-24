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

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqException;
import org.zeromq.messaging.ZmqMessage;
import org.zeromq.support.HasDestroy;
import org.zeromq.support.ObjectBuilder;
import org.zeromq.support.pool.Lease;
import org.zeromq.support.pool.ObjectPool;
import org.zeromq.support.pool.SimpleObjectPool;

import java.util.BitSet;
import java.util.UUID;

public final class SyncClient implements HasDestroy {

  private static final Logger LOG = LoggerFactory.getLogger(SyncClient.class);

  private static final long DEFAULT_RETRY_TIMEOUT = 3000; // timeout before give up and returning null.

  public static final class Builder implements ObjectBuilder<SyncClient> {

    private final SyncClient _target = new SyncClient();

    private Builder() {
    }

    public Builder withChannelBuilder(ZmqChannel.Builder channelBuilder) {
      _target.channelBuilder = channelBuilder;
      return this;
    }

    public Builder withRetryTimeout(long retryTimeout) {
      if (retryTimeout > 0) {
        _target.retryTimeout = retryTimeout;
      }
      return this;
    }

    public Builder withCorrIdProvider(ObjectBuilder<Long> corrIdProvider) {
      _target.corrIdProvider = corrIdProvider;
      return this;
    }

    @Override
    public void checkInvariant() {
      if (_target.corrIdProvider == null) {
        throw ZmqException.fatal();
      }
      if (_target.channelBuilder == null) {
        throw ZmqException.fatal();
      }
    }

    @Override
    public SyncClient build() {
      checkInvariant();
      ObjectBuilder<Client> clientBuilder = new ObjectBuilder<Client>() {
        @Override
        public void checkInvariant() {
          // no-op.
        }

        @Override
        public Client build() {
          return new Client(_target.channelBuilder.build(),
                            _target.retryTimeout,
                            _target.corrIdProvider);
        }
      };
      _target._clientPool = new SimpleObjectPool<Client>(clientBuilder);
      return _target;
    }
  }

  private static class Client {

    static final int SEND = 0;
    static final int RECV = 1;
    static final int RECV_REPLY = 2;
    static final int RECV_SEND_RETRY = 3;
    static final int RECV_REPLY_VALIDATE = 4;

    final ZmqChannel channel;
    final long retryTimeout;
    final ObjectBuilder<Long> corrIdProvider;

    final BitSet _fsm = new BitSet(5);
    Long _corrId;

    Client(ZmqChannel channel, long retryTimeout, ObjectBuilder<Long> corrIdProvider) {
      this.channel = channel;
      this.retryTimeout = retryTimeout;
      this.corrIdProvider = corrIdProvider;
      this.initFsm();
    }

    boolean send(ZmqMessage message) {
      setSend();

      message = ZmqMessage.builder(message)
                          .withHeaders(new ServiceHeaders()
                                           .copy(message.headers())
                                           .setCorrId(_corrId))
                          .build();

      return channel.send(message);
    }

    ZmqMessage recv() {
      setRecv();
      setRecvReply(); // initially FSM is set to "receive reply".

      Stopwatch timer = new Stopwatch().start();
      ZmqMessage message = null;
      ServiceHeaders headers = null;
      for (; ; ) {
        // receive reply.
        if (isRecvReply()) {
          message = channel.recv();
          if (message == null) {
            return null;
          }
          headers = message.headersAs(ServiceHeaders.class);
          setRecvReplyValidate();
        }
        // validate reply: check corresponding headers.
        if (isRecvReplyValidate()) {
          // check correlation.
          Long corrId = headers.getCorrId();
          if (_corrId.compareTo(corrId) != 0) {
            LOG.warn("Unrecognized correlation_id={} (expected {}).", corrId, _corrId);
            setRecvReply(); // recv reply again.
            continue;
          }
          // check headers.
          if (headers.isMsgTypeNotSet()) {
            return message;
          }
          else if (headers.isMsgTypeRetry()) {
            setRecvSendRetry();
          }
          else {
            String msgType = headers.getHeaderOrException(ServiceHeaders.HEADER_MSG_TYPE);
            LOG.error("!!! Unsupported msg_type={} detected.", msgType);
            throw ZmqException.wrongMessage();
          }
        }
        // got retry command: check a timer first.
        if (isRecvSendRetry()) {
          if (retryTimeout - timer.elapsedMillis() <= 0) {
            LOG.warn("Can't retry: timeout({} ms) exceeded. Returning null.", retryTimeout);
            return null;
          }
          boolean sent = channel.send(ZmqMessage.builder(message)
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

    void initFsm() {
      _fsm.set(SEND);
      _fsm.set(RECV);
    }

    boolean setSend() {
      _fsm.set(SEND);
      boolean wasRecv = _fsm.get(RECV);
      _fsm.clear(RECV);
      if (wasRecv) {
        // clear saved state.
        _corrId = null;
        // setup any new preparations for sending.
        _corrId = corrIdProvider.build();
      }
      return wasRecv;
    }

    boolean setRecv() {
      _fsm.set(RECV);
      boolean wasSend = _fsm.get(SEND);
      _fsm.clear(SEND);
      return wasSend;
    }

    void setRecvSendRetry() {
      _fsm.set(RECV_SEND_RETRY);
      _fsm.clear(RECV_REPLY);
      _fsm.clear(RECV_REPLY_VALIDATE);
    }

    boolean isRecvSendRetry() {
      return _fsm.get(RECV_SEND_RETRY);
    }

    void setRecvReply() {
      _fsm.set(RECV_REPLY);
      _fsm.clear(RECV_SEND_RETRY);
      _fsm.clear(RECV_REPLY_VALIDATE);
    }

    boolean isRecvReply() {
      return _fsm.get(RECV_REPLY);
    }

    void setRecvReplyValidate() {
      _fsm.set(RECV_REPLY_VALIDATE);
      _fsm.clear(RECV_REPLY);
      _fsm.clear(RECV_SEND_RETRY);
    }

    boolean isRecvReplyValidate() {
      return _fsm.get(RECV_REPLY_VALIDATE);
    }
  }

  private ZmqChannel.Builder channelBuilder;
  private long retryTimeout = DEFAULT_RETRY_TIMEOUT;
  private ObjectBuilder<Long> corrIdProvider = new ObjectBuilder<Long>() {
    @Override
    public void checkInvariant() {
      // no-op.
    }

    @Override
    public Long build() {
      return UUID.randomUUID().getMostSignificantBits(); // default UUID :|
    }
  };

  private Lease<Client> _l;
  private ObjectPool<Client> _clientPool;

  //// CONSTRUCTORS

  private SyncClient() {
  }

  //// METHODS

  public static Builder builder() {
    return new Builder();
  }

  public void lease() {
    _l = _clientPool.lease();
  }

  public void release() {
    _l.release();
    _l = null;
  }

  public boolean send(ZmqMessage message) {
    if (_l == null) {
      throw ZmqException.fatal();
    }
    return _l.get().send(message);
  }

  public ZmqMessage recv() {
    if (_l == null) {
      throw ZmqException.fatal();
    }
    return _l.get().recv();
  }

  @Override
  public void destroy() {
    this._clientPool.destroy();
  }
}
