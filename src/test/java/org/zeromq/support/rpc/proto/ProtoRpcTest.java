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

import com.google.protobuf.ByteString;
import org.junit.Test;
import org.springframework.test.context.ContextConfiguration;
import org.zeromq.messaging.ZmqException;
import org.zeromq.support.spring.SpringFixture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@ContextConfiguration(locations = {"classpath:/org/zeromq/support/rpc/proto/caller.xml",
                                   "classpath:/org/zeromq/support/rpc/proto/msg-processor.xml",
                                   "classpath:/org/zeromq/support/rpc/zmq-context.xml"})
public class ProtoRpcTest {

  static final String BEAN_ID = "cepasaService";

  @Test // org.zeromq.support.rpc.proto.Proto.Pair
  public void t0() {
    SpringFixture f = new SpringFixture().setup(ProtoRpcTest.class);
    try {
      CepasaService service = f.getBean(BEAN_ID);
      for (int iter = 0; iter < 100; iter++) {
        Proto.Pair res = service.cepasa(Proto.Collection.newBuilder().build());
        assertEquals("cepasa!", res.getKey().toStringUtf8());
      }
    }
    finally {
      f.cleanup();
    }
  }

  @Test
  public void t1() {
    SpringFixture f = new SpringFixture().setup(ProtoRpcTest.class);
    try {
      CepasaService service = f.getBean(BEAN_ID);
      for (int iter = 0; iter < 100; iter++) {
        try {
          service.cepasa(Proto.Pair.newBuilder()
                              .setKey(ByteString.EMPTY)
                              .setValue(ByteString.EMPTY)
                              .build());
          fail();
        }
        catch (Exception e) {
          assert e instanceof ZmqException;
        }
      }
    }
    finally {
      f.cleanup();
    }
  }
}
