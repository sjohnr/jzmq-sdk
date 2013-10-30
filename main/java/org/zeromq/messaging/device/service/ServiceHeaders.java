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
import org.zeromq.support.ZmqUtils;

import java.util.Arrays;

public final class ServiceHeaders extends ZmqHeaders {

  private static final int HEADER_ID_MSG_TYPE = "ServiceHeaders.HEADER_ID_MSG_TYPE".hashCode();
  private static final int HEADER_ID_MSG_NUM_OF_HOPS = "ServiceHeaders.HEADER_ID_MSG_NUM_OF_HOPS".hashCode();

  private static final byte[] HEADER_FRAME_PING = "PING".getBytes();

  //// METHODS

  public ServiceHeaders setMsgTypePing() {
    put(HEADER_ID_MSG_TYPE, HEADER_FRAME_PING);
    return this;
  }

  public boolean isMsgTypePing() {
    return Arrays.equals(HEADER_FRAME_PING, getHeaderOrNull(HEADER_ID_MSG_TYPE));
  }

  public ServiceHeaders setMsgTypeNotSet() {
    remove(HEADER_ID_MSG_TYPE);
    return this;
  }

  public boolean isMsgTypeNotSet() {
    return getHeaderOrNull(HEADER_ID_MSG_TYPE) == null;
  }

  public ServiceHeaders setNumOfHops(int numOfHops) {
    put(HEADER_ID_MSG_NUM_OF_HOPS, ZmqUtils.intAsBytes(numOfHops));
    return this;
  }

  public int getNumOfHops() throws ZmqException {
    return ZmqUtils.bytesAsInt(getHeaderOrException(HEADER_ID_MSG_NUM_OF_HOPS));
  }
}
