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
import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqMessage;
import org.zeromq.messaging.event.EventReceived;

public final class TryAgainEventListener {

  private static final long DEFAULT_TIMEOUT = 1000; // timeout before give up and returning null.

  private static final Logger LOG = LoggerFactory.getLogger(TryAgainEventListener.class);

  private long timeout = DEFAULT_TIMEOUT;

  //// CONSTRUCTORS

  public TryAgainEventListener() {
  }

  public TryAgainEventListener(long timeout) {
    if (timeout > 0) {
      this.timeout = timeout;
    }
  }

  //// METHODS

  @Subscribe
  @AllowConcurrentEvents
  public void event(EventReceived event) {
    ZmqMessage message = event.message();
    ServiceHeaders headers = message.headersAs(ServiceHeaders.class);

    if (headers.isMsgTypeNotSet()) {
      return; // this is regular reply.
    }
    if (!headers.isMsgTypeTryAgain()) {
      event.clear();
      LOG.error("Unsupported 'msg_type' detected. Message cleared.");
      return;
    }

    long nextTimeout = timeout - Math.max(event.elapsedTime(), 1);
    if (nextTimeout <= 0) {
      event.clear();
      LOG.warn("Can't TRY_AGAIN: timeout({} ms) exceeded.", timeout);
      return;
    }
    Stopwatch timer = new Stopwatch();
    for (int i = 1; ; i++) {
      LOG.debug("TRY_AGAIN detected. Calling.");
      timer.reset().start();
      ZmqChannel channel = event.channel();
      boolean sent = channel.send(createMsg(message));
      if (!sent) {
        event.clear();
        LOG.warn("Can't TRY_AGAIN: .send() failed!");
        return;
      }
      message = channel.recv();
      if (message == null) {
        event.clear();
        LOG.warn("Can't TRY_AGAIN: .recv() failed!");
        return;
      }
      else {
        headers = message.headersAs(ServiceHeaders.class);
        if (headers.isMsgTypeNotSet()) {
          event.replace(message);
          LOG.debug("Got reply after calling TRY_AGAIN {} times.", i);
          return;
        }
        if (headers.isMsgTypeTryAgain()) {
          nextTimeout -= Math.max(timer.stop().elapsedMillis(), 1);
          if (nextTimeout <= 0) {
            event.clear();
            LOG.warn("Can't TRY_AGAIN anymore({} attempts): timeout({} ms) exceeded.", i, timeout);
            return;
          }
        }
        else {
          event.clear();
          LOG.error("Unsupported 'msg_type' detected. Message cleared.");
          return;
        }
      }
    }
  }

  private ZmqMessage createMsg(ZmqMessage message) {
    return ZmqMessage.builder(message)
                     .withHeaders(new ServiceHeaders()
                                      .copy(message.headers())
                                      .setMsgTypeNotSet())
                     .build();
  }
}
