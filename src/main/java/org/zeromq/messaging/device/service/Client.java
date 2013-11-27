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

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public final class Client implements HasDestroy {

  private static final Logger LOG = LoggerFactory.getLogger(Client.class);

  private static final long DEFAULT_TIMEOUT = 1000; // timeout before give up and returning null.

  public static final class Builder implements ObjectBuilder<Client> {

    private final Client _target = new Client();

    private Builder() {
    }

    public Builder withChannelBuilder(ZmqChannel.Builder channelBuilder) {
      _target.channelBuilder = channelBuilder;
      return this;
    }

    public Builder withTryAgainTimeout(long tryAgainTimeout) {
      if (tryAgainTimeout > 0) {
        _target.tryAgainTimeout = tryAgainTimeout;
      }
      return this;
    }

    public Builder withCorrelator(ObjectBuilder<Long> correlator) {
      _target.correlator = correlator;
      return this;
    }

    @Override
    public void checkInvariant() {
      if (_target.correlator == null) {
        throw ZmqException.fatal();
      }
      if (_target.channelBuilder == null) {
        throw ZmqException.fatal();
      }
    }

    @Override
    public Client build() {
      checkInvariant();
      _target._channel = _target.channelBuilder.build();
      return _target;
    }
  }

  private static class State {

    static final int BIT_SENDING = 0;
    static final int BIT_RECEIVING = 1;

    final BitSet _op = new BitSet(2);

    /**
     * Sets current state to <i>sending</i>.
     *
     * @return true if prev state was <i>receiving</i>.
     */
    boolean setSending() {
      _op.set(BIT_SENDING);
      return clearReceiving();
    }

    /**
     * Sets current state to <i>receiving</i>.
     *
     * @return true if prev state was <i>sending</i>.
     */
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
  private long tryAgainTimeout = DEFAULT_TIMEOUT;
  private ObjectBuilder<Long> correlator = new ObjectBuilder<Long>() {
    @Override
    public void checkInvariant() {
      // no-op.
    }

    @Override
    public Long build() {
      return UUID.randomUUID().getMostSignificantBits(); // default UUID generation :|
    }
  };

  private ZmqChannel _channel;
  private final State _s = new State();
  private final Map<Long, AtomicLong> _history = new HashMap<Long, AtomicLong>(1);

  //// CONSTRUCTORS

  private Client() {
  }

  //// METHODS

  public static Builder builder() {
    return new Builder();
  }

  public boolean send(ZmqMessage message) {
    return _channel.send(trackCorrelation(message));
  }

  public ZmqMessage recv() {
    Stopwatch timer = new Stopwatch().start();
    ZmqMessage message = _channel.recv();
    timer.stop();

    if (message != null) {
      ServiceHeaders headers = message.headersAs(ServiceHeaders.class);
      int i = 0;
      long t = tryAgainTimeout;
      long nextTimeout;
      do {
        if (headers.isMsgTypeNotSet()) {
          keepCorrelation(headers);
          return message;
        }
        if (!headers.isMsgTypeTryAgain()) {
          LOG.error("Unsupported 'msg_type' detected. Returning null.");
          return null;
        }
        nextTimeout = t - Math.max(timer.elapsedMillis(), 1);
        if (nextTimeout <= 0) {
          LOG.warn("Can't TRYAGAIN: timeout({} ms) exceeded. Returning null.", tryAgainTimeout);
          return null;
        }

        LOG.debug("TRYAGAIN detected. Calling.");
        timer.reset().start();
        boolean sent = _channel.send(ZmqMessage.builder(message)
                                               .withHeaders(new ServiceHeaders()
                                                                .copy(message.headers())
                                                                .setMsgTypeNotSet())
                                               .build());
        if (!sent) {
          LOG.warn("Can't TRYAGAIN: .send() failed! Returning null.");
          return null;
        }
        message = _channel.recv();
        timer.stop();

        if (message == null) {
          LOG.warn("Can't TRYAGAIN: .recv() failed! Returning null.");
          return null;
        }

        i++;
        t = nextTimeout;
      }
      while (true);
    }

    return message;
  }

  @Override
  public void destroy() {
    if (_channel != null) {
      _channel.destroy();
    }
  }

  private ZmqMessage trackCorrelation(ZmqMessage message) {
    if (_s.setSending()) {
      _history.clear(); // TODO artemv: consider to generate CID here.
    }

    ServiceHeaders headers = message.headersAs(ServiceHeaders.class);
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

    return message;
  }

  private void keepCorrelation(ServiceHeaders headers) {
    Long corrId = headers.getCorrId();
    AtomicLong counter = _history.get(corrId);
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
