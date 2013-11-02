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

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.zeromq.messaging.ZmqException.ErrorCode.HEADER_IS_NOT_SET;
import static org.zeromq.messaging.ZmqException.ErrorCode.WRONG_HEADER;
import static org.zeromq.messaging.ZmqMessage.DIV_FRAME;
import static org.zeromq.messaging.ZmqMessage.EMPTY_FRAME;

public class ZmqHeadersTest {

  @Test
  public void t0() {
    ZmqHeaders headers = new ZmqHeaders();

    assertArrayEquals(EMPTY_FRAME, headers.asBinary());

    assert headers.remove("").isEmpty();
    assert headers.getHeaderOrNull("").isEmpty();
  }

  @Test
  public void t1() {
    ZmqHeaders headers = new ZmqHeaders().copy("{\"0\":[\"abc\"]}".getBytes());

    headers.getHeaderOrException("0"); // expecting header.
    assert headers.getHeaderOrNull("1").isEmpty(); // not expecting header.
    try {
      headers.getHeaderOrException("2"); // expecting exception.
      fail();
    }
    catch (ZmqException e) {
      assertEquals(HEADER_IS_NOT_SET, e.errorCode());
    }
  }

  @Test
  public void t2() {
    ZmqHeaders headers = new ZmqHeaders().copy(new ZmqHeaders())
                                         .copy(new ZmqHeaders())
                                         .copy(new ZmqHeaders())
                                         .copy(new ZmqHeaders());

    assertArrayEquals(EMPTY_FRAME, headers.asBinary());
  }

  @Test
  public void t3() {
    ZmqHeaders headers = new ZmqHeaders().copy(new ZmqHeaders().set("0", 0))
                                         .copy(new ZmqHeaders().set("1", 1))
                                         .copy(new ZmqHeaders().set("2", 2))
                                         .set("3", "a")
                                         .set("4", "b")
                                         .set("5", "c");

    assertEquals("0", headers.getHeaderOrNull("0").iterator().next());
    assertEquals("1", headers.getHeaderOrNull("1").iterator().next());
    assertEquals("2", headers.getHeaderOrNull("2").iterator().next());
    assertEquals("a", headers.getHeaderOrNull("3").iterator().next());
    assertEquals("b", headers.getHeaderOrNull("4").iterator().next());
    assertEquals("c", headers.getHeaderOrNull("5").iterator().next());
  }

  @Test
  public void t4() {
    new ZmqHeaders().copy(EMPTY_FRAME);

    new ZmqHeaders().copy(new byte[0]);

    try {
      new ZmqHeaders().copy(DIV_FRAME);
      fail();
    }
    catch (ZmqException e) {
      assert e.errorCode() == WRONG_HEADER;
    }

    try {
      new ZmqHeaders().set("x", "");
      fail();
    }
    catch (IllegalArgumentException e) {
    }

    try {
      new ZmqHeaders().copy("{\"0\":[]}".getBytes());
      fail();
    }
    catch (ZmqException e) {
      assert e.errorCode() == WRONG_HEADER;
    }

    try {
      new ZmqHeaders().copy("{\"0\":[\"\"]}".getBytes());
      fail();
    }
    catch (ZmqException e) {
      assert e.errorCode() == WRONG_HEADER;
    }
  }

  @Test
  public void t5() {
    ZmqHeaders headers = new ZmqHeaders().set("a", "a")
                                         .set("b", "b")
                                         .set("c", "c");

    assertArrayEquals("{\"a\":[\"a\"],\"b\":[\"b\"],\"c\":[\"c\"]}".getBytes(), headers.asBinary());
  }
}
