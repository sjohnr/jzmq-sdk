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

package org.zeromq.support.rpc.javanative;

import java.io.Serializable;

class Reply implements Serializable {

  private static final long serialVersionUID = Reply.class.getName().hashCode();

  public static final Reply VoidReply = new Reply(new byte[0]);

  private Type replyType;
  private Error error;
  private byte[] result;

  //// CONSTRUCTORS

  public Reply() {
  }

  Reply(byte[] result) {
    assert result != null;
    this.result = result;
    this.replyType = Type.NORMAL;
    this.error = null;
  }

  Reply(Error error) {
    assert error != null;
    this.error = error;
    this.replyType = Type.ERROR;
    this.result = null;
  }

  //// METHODS

  public Error error() {
    return error;
  }

  public Type replyType() {
    return replyType;
  }

  public byte[] result() {
    return result;
  }
}
