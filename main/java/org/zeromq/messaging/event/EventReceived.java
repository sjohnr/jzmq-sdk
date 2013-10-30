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

package org.zeromq.messaging.event;

import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqMessage;

public final class EventReceived extends ZmqAbstractTimerEvent {

  /**
   * A message which was received.
   * <p/>
   * <b>NOTE: can be <b>reset</b> by event_listeners using {@link #replace(ZmqMessage)}.</b>
   */
  private ZmqMessage message;

  public EventReceived(ZmqChannel channel, ZmqMessage message, Long elapsedTime) {
    super(channel, elapsedTime);
    assert message != null;
    this.message = message;
  }

  /** @return {@link #message}. May return null. */
  public ZmqMessage message() {
    return message;
  }

  public void replace(ZmqMessage message) {
    assert message != null;
    this.message = message;
  }

  /** Invalidates received message. Setting {@link #message} to {@code null}. */
  public void clear() {
    this.message = null;
  }
}
