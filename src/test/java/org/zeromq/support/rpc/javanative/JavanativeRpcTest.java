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

import com.google.common.base.Throwables;
import org.junit.Test;
import org.springframework.test.context.ContextConfiguration;
import org.zeromq.messaging.ZmqException;
import org.zeromq.support.spring.SpringFixture;

import java.rmi.RemoteException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@ContextConfiguration(locations = {"classpath:/org/zeromq/support/rpc/javanative/msg-processor.xml",
                                   "classpath:/org/zeromq/support/rpc/javanative/caller.xml",
                                   "classpath:/org/zeromq/support/rpc/zmq-context.xml"})
public class JavanativeRpcTest {

  static final String BEAN_ID = "origatoService";

  @Test
  public void t0() {
    SpringFixture f = new SpringFixture().setup(JavanativeRpcTest.class);
    try {
      OrigatoService service = f.getBean(BEAN_ID);
      int arg_0 = 1;
      String[] arg_1 = {"one", "abc"};
      long timestamp = System.currentTimeMillis();
      for (int iter = 0; iter < 100; iter++) {
        assertEquals("origato-1-" + timestamp + "-one-abc", service.origato(arg_0, arg_1, timestamp));
        assertEquals("abc-xyz", service.origato(null, null, null));
        service.emptyMethod();
      }
    }
    finally {
      f.cleanup();
    }
  }

  @Test
  public void t1() {
    SpringFixture f = new SpringFixture().setup(JavanativeRpcTest.class);
    try {
      OrigatoService service = f.getBean(BEAN_ID);
      for (int iter = 0; iter < 100; iter++) {
        try {
          service.raiseRuntimeException();
          fail();
        }
        catch (RuntimeException e) {
          assertErrorCode((ZmqException) e);
        }
        try {
          service.raiseCheckedException();
          fail();
        }
        catch (RemoteException e) {
          fail();
        }
        catch (ZmqException e) {
          assertErrorCode(e);
        }
        try {
          service.raiseAssertionError();
          fail();
        }
        catch (ZmqException e) {
          assertErrorCode(e);
        }
      }
    }
    finally {
      f.cleanup();
    }
  }

  private void assertErrorCode(ZmqException e) {
    assertEquals(ZmqException.ErrorCode.SEE_CAUSE, e.errorCode());
    assert Throwables.getRootCause(e) instanceof RuntimeException;
  }
}
