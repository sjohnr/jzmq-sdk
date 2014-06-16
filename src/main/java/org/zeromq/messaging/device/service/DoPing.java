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

import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqHeaders;
import org.zeromq.messaging.ZmqMessage;

import static org.zeromq.messaging.ZmqCommonHeaders.Header.ZMQ_MESSAGE_TYPE;
import static org.zeromq.messaging.ZmqCommonHeaders.ZMQ_MESSAGE_TYPE_PING;

class DoPing implements ZmqPingStrategy {

  private static final ZmqMessage PING;

  static {
    PING = ZmqMessage.builder()
                     .withHeaders(ZmqHeaders.builder()
                                            .set(ZMQ_MESSAGE_TYPE.id(), ZMQ_MESSAGE_TYPE_PING)
                                            .build()
                                            .asBinary())
                     .build();
  }

  @Override
  public void ping(ZmqChannel channel) {
    channel.send(PING);
  }
}
