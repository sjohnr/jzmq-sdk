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

package org.zeromq.support;

import org.junit.Test;
import org.zeromq.messaging.ZmqFrames;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.zeromq.support.ZmqUtils.bytesAsInt;
import static org.zeromq.support.ZmqUtils.bytesAsLong;
import static org.zeromq.support.ZmqUtils.intAsBytes;
import static org.zeromq.support.ZmqUtils.longAsBytes;
import static org.zeromq.support.ZmqUtils.mergeBytes;

public class ZmqUtilsTest {

  @Test
  public void t0() {
    int expected = Integer.MAX_VALUE;
    byte[] buf = intAsBytes(expected);
    assertEquals(expected, bytesAsInt(buf));
  }

  @Test
  public void t1() {
    long expected = Long.MAX_VALUE;
    byte[] buf = longAsBytes(expected);
    assertEquals(expected, bytesAsLong(buf));
  }

  @Test
  public void t2() {
    byte[] bytes = mergeBytes(Arrays.asList("AAA".getBytes(),
                                            "BBB".getBytes(),
                                            "CCC".getBytes()));
    // AAA
    assertEquals(65, bytes[0]);
    assertEquals(65, bytes[1]);
    assertEquals(65, bytes[2]);
    // BBB
    assertEquals(66, bytes[3]);
    assertEquals(66, bytes[4]);
    assertEquals(66, bytes[5]);
    // CCC
    assertEquals(67, bytes[6]);
    assertEquals(67, bytes[7]);
    assertEquals(67, bytes[8]);
  }

  @Test
  public void t3() {
    ZmqFrames identities = new ZmqFrames();
    identities.add(new byte[]{0, 0, 0, 24, -63});
    try {
      bytesAsLong(mergeBytes(identities));
      fail();
    }
    catch (BufferUnderflowException e) {
    }

    identities = new ZmqFrames();
    identities.add(new byte[]{0, 0, 0, 24, -63, 12, 12, 12});
    bytesAsLong(mergeBytes(identities));

    identities = new ZmqFrames();
    identities.add(new byte[]{0, 0, 0, 24, -63, 12, 12, 12, 0, 0, 0, 24, -63, 12, 12, 12});
    try {
      bytesAsLong(mergeBytes(identities));
      fail();
    }
    catch (BufferOverflowException e) {
    }
  }

  @Test
  public void t4() {
    ZmqFrames identities = new ZmqFrames();
    identities.add(new byte[]{24, -63});
    try {
      bytesAsInt(mergeBytes(identities));
      fail();
    }
    catch (BufferUnderflowException e) {
    }

    identities = new ZmqFrames();
    identities.add(new byte[]{24, -63, 12, 12});
    bytesAsInt(mergeBytes(identities));

    identities = new ZmqFrames();
    identities.add(new byte[]{24, -63, 12, 12, 24, -63, 12, 12});
    try {
      bytesAsInt(mergeBytes(identities));
      fail();
    }
    catch (BufferOverflowException e) {
    }
  }
}
