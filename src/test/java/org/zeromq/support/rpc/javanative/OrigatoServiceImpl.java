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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.rmi.RemoteException;

public class OrigatoServiceImpl implements OrigatoService {

  @Override
  public String origato(Integer x, String[] y, Long z) {
    if (x == null && (y == null || y.length == 0) && z == null) {
      return "abc-xyz";
    }
    return Joiner.on('-').join(Iterables.concat(
        ImmutableList.of("origato", x, z),
        ImmutableList.copyOf(y)));
  }

  @Override
  public void emptyMethod() {
    // no-op.
  }

  @Override
  public void raiseRuntimeException() throws RuntimeException {
    throw new RuntimeException("Thank you. Good bye.");
  }

  @Override
  public void raiseCheckedException() throws RemoteException {
    throw new RemoteException("Thank you. Good bye.");
  }

  @Override
  public void raiseAssertionError() throws AssertionError {
    throw new AssertionError("Thank you. Good bye.");
  }
}
