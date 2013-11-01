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

class Error implements Serializable {

  private static final long serialVersionUID = Error.class.getName().hashCode();

  private Integer code;
  private String message;

  //// CONSTRUCTORS

  public Error() {
  }

  Error(Integer code) {
    this(code, null);
  }

  Error(Integer code, Throwable t) {
    assert code != null;
    this.code = code;
    if (t == null) {
      this.message = null;
    }
    else {
      this.message = t.getMessage() == null ?
                     t.getClass().getCanonicalName() :
                     t.getMessage();
    }
  }

  //// METHODS

  public Integer code() {
    return code;
  }

  public String message() {
    return message;
  }
}