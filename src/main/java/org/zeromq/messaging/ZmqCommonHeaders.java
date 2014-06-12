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

package org.zeromq.messaging;

import static org.zeromq.messaging.ZmqCommonHeaders.HeaderName.ZMQ_MESSAGE_CID;
import static org.zeromq.messaging.ZmqCommonHeaders.HeaderName.ZMQ_MESSAGE_TYPE;

/** Common ZMQ message headers. */
public final class ZmqCommonHeaders extends ZmqHeaders<ZmqCommonHeaders> {

  public static enum HeaderName {
    ZMQ_MESSAGE_TYPE,
    ZMQ_MESSAGE_CID
  }

  public static enum MessageType {
    PING,
    PONG
  }

  //// METHODS

  public ZmqCommonHeaders setMessageType(MessageType messageType) {
    set(ZMQ_MESSAGE_TYPE.name(), messageType.name());
    return this;
  }

  public MessageType getMessageType() {
    String messageType = getHeaderOrNull(ZMQ_MESSAGE_TYPE.name());
    return messageType != null ? MessageType.valueOf(messageType) : null;
  }

  public boolean isMessageTypeNotSet() {
    return getHeaderOrNull(ZMQ_MESSAGE_TYPE.name()) == null;
  }

  public ZmqCommonHeaders setCid(String cid) {
    set(ZMQ_MESSAGE_CID.name(), cid);
    return this;
  }

  public String getCid() {
    return getHeaderOrNull(ZMQ_MESSAGE_CID.name());
  }
}
