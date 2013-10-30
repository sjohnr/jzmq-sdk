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

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.zeromq.support.ZmqUtils.intAsBytes;
import static org.zeromq.support.ZmqUtils.mergeBytes;

public class ZmqHeadersTest {

  @Test
  public void t0() {
    ZmqHeaders headers = new ZmqHeaders();
    assert headers.getHeaderOrNull(0) == null;
    assert headers.remove(1) == null;
    assertEquals(0, headers.asFrames().size());
  }

  @Test
  public void t1() {
    ZmqHeaders headers = new ZmqHeaders();

    ZmqFrames frames = new ZmqFrames();
    frames.add(mergeBytes(Arrays.asList(intAsBytes(0), "header_content".getBytes())));
    headers.put(frames);

    headers.getHeaderOrException(0); // expecting header.
    assert headers.getHeaderOrNull(1) == null; // not expecting header.
    try {
      headers.getHeaderOrException(2); // expecting exception.
      fail();
    }
    catch (ZmqException e) {
      assertEquals(ZmqException.ErrorCode.HEADER_IS_NOT_SET, e.errorCode());
    }
  }

  @Test
  public void t2() {
    ZmqHeaders headers = new ZmqHeaders().copyOf(new ZmqHeaders())
                                         .copyOf(new ZmqHeaders())
                                         .copyOf(new ZmqHeaders())
                                         .copyOf(new ZmqHeaders());
    assertEquals(0, headers.asFrames().size());
  }

  @Test
  public void t3() {
    ZmqHeaders headers = new ZmqHeaders().copyOf(new ZmqHeaders().put(0, "0000".getBytes()))
                                         .copyOf(new ZmqHeaders().put(1, "1111".getBytes()))
                                         .copyOf(new ZmqHeaders().put(2, "2222".getBytes()))
                                         .put(3, "a".getBytes())
                                         .put(4, "b".getBytes())
                                         .put(5, "c".getBytes());
    assertEquals(6, headers.asFrames().size());

    ZmqFrames frames = new ZmqFrames();
    frames.add(mergeBytes(Arrays.asList(intAsBytes(100500), "header_content".getBytes())));
    headers.put(frames);
    assertEquals(6 + 1, headers.asFrames().size());
    assert Arrays.equals("0000".getBytes(), headers.getHeaderOrNull(0));
    assert Arrays.equals("1111".getBytes(), headers.getHeaderOrNull(1));
    assert Arrays.equals("2222".getBytes(), headers.getHeaderOrNull(2));
    assert Arrays.equals("a".getBytes(), headers.getHeaderOrNull(3));
    assert Arrays.equals("b".getBytes(), headers.getHeaderOrNull(4));
    assert Arrays.equals("c".getBytes(), headers.getHeaderOrNull(5));
    assert Arrays.equals("header_content".getBytes(), headers.getHeaderOrNull(100500));
  }

  @Test
  public void t4() {
    try {
      ZmqFrames emptyFrame = new ZmqFrames();
      emptyFrame.add(ZmqMessage.EMPTY_FRAME);
      new ZmqHeaders().put(emptyFrame);
      fail();
    }
    catch (AssertionError e) {
    }

    try {
      ZmqFrames divFrame = new ZmqFrames();
      divFrame.add(ZmqMessage.DIV_FRAME);
      new ZmqHeaders().put(divFrame);
      fail();
    }
    catch (AssertionError e) {
    }
  }

  @Test
  public void t5() {
    ZmqHeaders headers = new ZmqHeaders().put(0, "to_be_removed".getBytes())
                                         .put(1, "remained".getBytes());
    assertEquals(2, headers.asFrames().size());

    headers.remove(0);

    assertEquals(1, headers.asFrames().size());
    assert Arrays.equals("remained".getBytes(), headers.getHeaderOrNull(1));

    ZmqHeaders headersCopy = new ZmqHeaders().copyOf(headers);
    headersCopy.remove(1);

    assertEquals(1, headers.asFrames().size()); // original not changed.
    assert Arrays.equals("remained".getBytes(), headers.getHeaderOrNull(1)); // original not changed.
    assertEquals(0, headersCopy.asFrames().size()); // copy has been changed.
  }

  @Test
  public void t6() {
    ZmqHeaders headers = new ZmqHeaders().put(0, "x".getBytes())
                                         .put(1, "y".getBytes())
                                         .put(2, "z".getBytes());

    assertEquals(3, headers.asFrames().size());
    assert Arrays.equals("x".getBytes(), headers.getHeaderOrNull(0));
    assert Arrays.equals("y".getBytes(), headers.getHeaderOrNull(1));
    assert Arrays.equals("z".getBytes(), headers.getHeaderOrNull(2));

    ZmqHeaders headersCopy = new ZmqHeaders().copyOf(headers);

    assertEquals(3, headersCopy.asFrames().size());
    assert Arrays.equals("x".getBytes(), headersCopy.getHeaderOrNull(0));
    assert Arrays.equals("y".getBytes(), headersCopy.getHeaderOrNull(1));
    assert Arrays.equals("z".getBytes(), headersCopy.getHeaderOrNull(2));
  }
}
