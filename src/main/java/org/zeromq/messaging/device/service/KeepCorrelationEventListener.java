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

import com.google.common.base.Preconditions;
import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.messaging.ZmqException;
import org.zeromq.messaging.ZmqMessage;
import org.zeromq.messaging.event.EventBeforeSend;
import org.zeromq.messaging.event.EventReceived;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public final class KeepCorrelationEventListener {

  private static final Logger LOG = LoggerFactory.getLogger(KeepCorrelationEventListener.class);

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

  private final State _s = new State();
  private final Map<Long, AtomicLong> _history = new HashMap<Long, AtomicLong>(1);

  @Subscribe
  @AllowConcurrentEvents
  public void event0(EventBeforeSend event) {
    if (_s.setSending()) {
      _history.clear();
    }

    ZmqMessage message = event.message();
    ServiceHeaders headers = message.headersAs(ServiceHeaders.class);
    Long corrId = headers.getCorrId();
    AtomicLong l = _history.get(corrId);

    if (l == null) {
      _history.put(corrId, l = new AtomicLong());
      if (_history.size() > 1) {
        LOG.error("!!! Multiple 'correlation_id'-s detected.");
        throw ZmqException.wrongMessage();
      }
    }
    l.incrementAndGet();
  }

  @Subscribe
  @AllowConcurrentEvents
  public void event1(EventReceived event) {
    Preconditions.checkState(_s.setReceiving(), "don't call .recv() before .send()!");

    ZmqMessage message = event.message();
    ServiceHeaders headers = message.headersAs(ServiceHeaders.class);
    Long corrId = headers.getCorrId();
    AtomicLong l = _history.get(corrId);

    if (l == null) {
      LOG.warn("Unrecognized 'correlation_id'. Message cleared.");
      event.clear();
    }
    else {
      l.decrementAndGet();
    }
  }
}
