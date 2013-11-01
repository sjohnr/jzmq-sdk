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

public abstract class ZmqAbstractTopicEvent extends ZmqAbstractChannelEvent {

  /**
   * Used by subscribe/unsubscribe event.
   * <p/>
   * <b>NOTE: this field is not optional.</b>
   */
  protected final byte[] topic;

  protected ZmqAbstractTopicEvent(ZmqChannel channel, byte[] topic) {
    super(channel);
    assert topic != null;
    this.topic = topic;
  }

  public final byte[] topic() {
    return topic;
  }
}