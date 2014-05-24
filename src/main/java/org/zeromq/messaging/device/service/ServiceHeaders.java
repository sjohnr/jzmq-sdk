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

import org.zeromq.messaging.ZmqHeaders;

/**
 * Service class headers. Acts as a mutable container for <i>service protocol</i> properties:
 * <pre>
 *   ServiceHeaders.MsgType -- PING/PONG.
 *   ServiceHeaders.MsgCid -- correlation id for call.
 * </pre>
 */
public final class ServiceHeaders extends ZmqHeaders<ServiceHeaders> {

  public static final String HEADER_MSG_TYPE = "ServiceHeaders.MsgType";
  public static final String HEADER_CID = "ServiceHeaders.MsgCid";

  private static final String PING = "PING";
  private static final String PONG = "PONG";

  //// METHODS

  public ServiceHeaders setMsgTypeNotSet() {
    remove(HEADER_MSG_TYPE);
    return this;
  }

  public boolean isMsgTypeNotSet() {
    return getHeaderOrNull(HEADER_MSG_TYPE) == null;
  }

  public ServiceHeaders setMsgTypePing() {
    set(HEADER_MSG_TYPE, PING);
    return this;
  }

  public ServiceHeaders setMsgTypePong() {
    set(HEADER_MSG_TYPE, PONG);
    return this;
  }

  public boolean isMsgTypePing() {
    return PING.equals(getHeaderOrNull(HEADER_MSG_TYPE));
  }

  public boolean isMsgTypePong() {
    return PONG.equals(getHeaderOrNull(HEADER_MSG_TYPE));
  }

  public ServiceHeaders setCid(long cid) {
    set(HEADER_CID, cid);
    return this;
  }

  public long getCid() {
    return Long.valueOf(getHeaderOrException(HEADER_CID));
  }
}
