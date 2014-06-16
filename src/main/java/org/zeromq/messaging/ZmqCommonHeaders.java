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

import java.util.regex.Pattern;

/** Common ZMQ message headers. */
public final class ZmqCommonHeaders {

  public static final String ZMQ_MESSAGE_TYPE_PING = "PING";

  public static enum Header {
    ZMQ_MESSAGE_TYPE("zmq_msg_type", Pattern.compile("zmq_msg_type=(\\w*+)[,]?"));

    private final String id;
    private final Pattern pattern;

    private Header(String id, Pattern pattern) {
      this.id = id;
      this.pattern = pattern;
    }

    public String id() {
      return id;
    }

    public Pattern pattern() {
      return pattern;
    }
  }

  private ZmqCommonHeaders() {
  }
}
