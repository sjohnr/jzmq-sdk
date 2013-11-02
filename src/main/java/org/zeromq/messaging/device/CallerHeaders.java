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

package org.zeromq.messaging.device;

import org.zeromq.messaging.ZmqHeaders;

import java.util.Collection;

public final class CallerHeaders extends ZmqHeaders {

  private static final String HEADER_MSG_TYPE = "Caller.MsgType";
  private static final String HEADER_CORRELATION_ID = "Caller.CorrId";

  private static final String TRYAGAIN = "TRYAGAIN";

  //// METHODS

  public CallerHeaders setMsgTypeTryAgain() {
    set(HEADER_MSG_TYPE, TRYAGAIN);
    return this;
  }

  public boolean isMsgTypeTryAgain() {
    Collection<String> c = getHeaderOrNull(HEADER_MSG_TYPE);
    return !c.isEmpty() && TRYAGAIN.equals(c.iterator().next());
  }

  public CallerHeaders setMsgTypeNotSet() {
    remove(HEADER_MSG_TYPE);
    return this;
  }

  public boolean isMsgTypeNotSet() {
    return getHeaderOrNull(HEADER_MSG_TYPE).isEmpty();
  }

  public CallerHeaders setCorrId(long id) {
    set(HEADER_CORRELATION_ID, id);
    return this;
  }

  public Long getCorrId() {
    Collection<String> c = getHeaderOrException(HEADER_CORRELATION_ID);
    return Long.valueOf(c.iterator().next());
  }
}
