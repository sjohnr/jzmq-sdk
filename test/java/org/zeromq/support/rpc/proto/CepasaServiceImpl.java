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

package org.zeromq.support.rpc.proto;

import static com.google.protobuf.ByteString.copyFromUtf8;

public class CepasaServiceImpl implements CepasaService {

  @Override
  public Proto.Pair cepasa(Proto.Collection arg) {
    return Proto.Pair.newBuilder()
                .setKey(copyFromUtf8("cepasa!"))
                .setValue(copyFromUtf8(""))
                .build();
  }

  @Override
  public Proto.Collection cepasa(Proto.Pair arg) {
    throw new RuntimeException("Thank you. Good bye.");
  }
}
