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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public final class SyncClient implements HasDestroy {

  private static final Logger LOG = LoggerFactory.getLogger(SyncClient.class);

  private static final long DEFAULT_RETRY_TIMEOUT = 1000; // timeout before give up and returning null.

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

    public Builder withCorrelation(ObjectBuilder<Long> correlation) {
      _target.correlation = correlation;
      return this;
    }

    @Override
    public void checkInvariant() {
      if (_target.correlation == null) {
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
                            _target.correlation);
        }
      };
      _target._clientPool = new SimpleObjectPool<Client>(clientBuilder);
      return _target;
    }
  }

  private static class Client {

    final ZmqChannel channel;
    final long retryTimeout;
    final ObjectBuilder<Long> correlation;

    final State _state = new State();
    final Map<Long, AtomicLong> _history = new HashMap<Long, AtomicLong>(1);

    Client(ZmqChannel channel, long retryTimeout, ObjectBuilder<Long> correlation) {
      this.channel = channel;
      this.retryTimeout = retryTimeout;
      this.correlation = correlation;
    }

    boolean send(ZmqMessage message) {
      ServiceHeaders headers = message.headersAs(ServiceHeaders.class);
      if (_state.isInitial() || _state.setSending()) {
        _history.clear();
        if (headers.getHeaderOrNull(ServiceHeaders.HEADER_CORRELATION_ID) == null) {
          Long corrId = correlation.build();
          headers = new ServiceHeaders().copy(headers).setCorrId(corrId);
          message = ZmqMessage.builder(message)
                              .withHeaders(headers)
                              .build();
        }
      }
      handleCorrelationOnSend(headers);
      return channel.send(message);
    }

    ZmqMessage recv() {
      _state.setReceiving();

      Stopwatch timer = new Stopwatch().start();
      ZmqMessage message = channel.recv();
      timer.stop();

      if (message != null) {
        ServiceHeaders headers = message.headersAs(ServiceHeaders.class);
        int i = 0;
        long t = retryTimeout;
        long nextTimeout;
        do {
          if (headers.isMsgTypeNotSet()) {
            handleCorrelationOnRecv(headers);
            return message;
          }
          if (!headers.isMsgTypeRetry()) {
            LOG.error("Unsupported 'msg_type' detected. Returning null.");
            return null;
          }
          nextTimeout = t - Math.max(timer.elapsedMillis(), 1);
          if (nextTimeout <= 0) {
            LOG.warn("Can't retry: timeout({} ms) exceeded. Returning null.", retryTimeout);
            return null;
          }

          LOG.debug("Retry detected. Calling.");
          timer.reset().start();
          boolean sent = channel.send(ZmqMessage.builder(message)
                                                .withHeaders(new ServiceHeaders()
                                                                 .copy(message.headers())
                                                                 .setMsgTypeNotSet())
                                                .build());
          if (!sent) {
            LOG.warn("Can't retry: .send() failed! Returning null.");
            return null;
          }
          message = channel.recv();
          timer.stop();

          if (message == null) {
            LOG.warn("Can't retry: .recv() failed! Returning null.");
            return null;
          }

          i++;
          t = nextTimeout;
        }
        while (true);
      }

      return message;
    }

    void handleCorrelationOnSend(ServiceHeaders headers) {
      Long corrId = headers.getCorrId();
      AtomicLong counter = _history.get(corrId);
      if (counter == null) {
        _history.put(corrId, counter = new AtomicLong());
        if (_history.size() > 1) {
          LOG.error("!!! Multiple 'correlation_id'-s detected.");
          throw ZmqException.wrongMessage();
        }
      }
      counter.incrementAndGet();
    }

    void handleCorrelationOnRecv(ServiceHeaders headers) {
      AtomicLong counter = _history.get(headers.getCorrId());
      if (counter == null) {
        LOG.error("!!! Unrecognized 'correlation_id'.");
        throw ZmqException.wrongMessage();
      }
      else {
        if (counter.decrementAndGet() == 0) {
          _history.clear();
        }
      }
    }
  }

  private static class State {

    static final int BIT_SENDING = 0;
    static final int BIT_RECEIVING = 1;

    final BitSet _op = new BitSet(2);

    boolean isInitial() {
      return _op.cardinality() == 0;
    }

    boolean setSending() {
      _op.set(BIT_SENDING);
      return clearReceiving();
    }

    boolean setReceiving() {
      _op.set(BIT_RECEIVING);
      return clearSending();
    }

    boolean clearSending() {
      boolean isSending = _op.get(BIT_SENDING);
      _op.clear(BIT_SENDING);
      return isSending;
    }

    boolean clearReceiving() {
      boolean isReceiving = _op.get(BIT_RECEIVING);
      _op.clear(BIT_RECEIVING);
      return isReceiving;
    }
  }

  private ZmqChannel.Builder channelBuilder;
  private long retryTimeout = DEFAULT_RETRY_TIMEOUT;
  private ObjectBuilder<Long> correlation = new ObjectBuilder<Long>() {
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
