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
import org.zeromq.messaging.event.EventReceived;
import org.zeromq.messaging.event.EventSent;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public final class KeepCorrelationEventListener {

  private static final Logger LOG = LoggerFactory.getLogger(KeepCorrelationEventListener.class);

  private static class State {

    final BitSet _op = new BitSet(2);

    boolean setSending() {
      _op.set(0);
      return clearReceiving();
    }

    boolean setReceiving() {
      _op.set(1);
      return clearSending();
    }

    boolean clearSending() {
      boolean wasSending = isSending();
      _op.clear(0);
      return wasSending;
    }

    boolean clearReceiving() {
      boolean wasReceiving = isReceiving();
      _op.clear(1);
      return wasReceiving;
    }

    boolean isSending() {
      return _op.get(0);
    }

    boolean isReceiving() {
      return _op.get(1);
    }
  }

  private final State _state = new State();
  private final Map<Long, AtomicLong> _history = new HashMap<Long, AtomicLong>(1);

  @Subscribe
  @AllowConcurrentEvents
  public void even0(EventSent event) {
    ZmqMessage message = event.message();
    ServiceHeaders headers = message.headersAs(ServiceHeaders.class);
    Long corrId = headers.getCorrId();
    boolean reset = _state.setSending();
    if (reset) {
      _history.clear();
    }
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
    ZmqMessage message = event.message();
    ServiceHeaders headers = message.headersAs(ServiceHeaders.class);
    Long corrId = headers.getCorrId();
    Preconditions.checkState(_state.setReceiving(), "don't call .recv() before .send().");
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
