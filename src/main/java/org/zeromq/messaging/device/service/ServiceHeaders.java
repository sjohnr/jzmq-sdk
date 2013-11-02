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

import org.zeromq.messaging.ZmqException;
import org.zeromq.messaging.ZmqHeaders;

import java.util.Collection;

public final class ServiceHeaders extends ZmqHeaders {

  private static final String HEADER_MSG_TYPE = "Service.MsgType";
  private static final String HEADER_MSG_NUM_OF_HOPS = "Service.MsgNumOfHops";

  private static final String PING = "PING";

  //// METHODS

  public ServiceHeaders setMsgTypePing() {
    set(HEADER_MSG_TYPE, PING);
    return this;
  }

  public boolean isMsgTypePing() {
    Collection<String> c = getHeaderOrNull(HEADER_MSG_TYPE);
    return !c.isEmpty() && PING.equals(c.iterator().next());
  }

  public ServiceHeaders setMsgTypeNotSet() {
    remove(HEADER_MSG_TYPE);
    return this;
  }

  public boolean isMsgTypeNotSet() {
    return getHeaderOrNull(HEADER_MSG_TYPE).isEmpty();
  }

  public ServiceHeaders setNumOfHops(int numOfHops) {
    set(HEADER_MSG_NUM_OF_HOPS, numOfHops);
    return this;
  }

  public int getNumOfHops() throws ZmqException {
    Collection<String> c = getHeaderOrException(HEADER_MSG_NUM_OF_HOPS);
    return Integer.valueOf(c.iterator().next());
  }
}
