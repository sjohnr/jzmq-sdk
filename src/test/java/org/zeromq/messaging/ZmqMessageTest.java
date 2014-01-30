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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.zeromq.messaging.ZmqMessage.DIV_FRAME;
import static org.zeromq.messaging.ZmqMessage.EMPTY_FRAME;

public class ZmqMessageTest {

  private static final byte[] id0 = "id0".getBytes();
  private static final byte[] id1 = "id1".getBytes();
  private static final byte[] id2 = "id2".getBytes();

  private static final String headerOverride0 = "h0";
  private static final String headerOverride1 = "h1";

  private static final byte[] topic = "topic".getBytes();

  private static final ZmqFrames identities = new ZmqFrames();

  static {
    identities.add(id0);
    identities.add(id1);
    identities.add(id2);
  }

  private static final byte[] headers = "{\"a\":[\"a\"]}".getBytes();
  private static final byte[] payload = "payload".getBytes();

  @Test
  public void t0() {
    ZmqMessage message = ZmqMessage.builder()
                                   .withTopic(topic)
                                   .withPayload(payload)
                                   .build();

    assertEq(topic, message.topic());
    assertEquals(0, message.identityFrames().size());
    assertArrayEquals(EMPTY_FRAME, message.headersAsBinary());
    assertEq(payload, message.payload());

    assertEq(message, ZmqMessage.builder(message).build());
  }

  @Test
  public void t1() {
    ZmqMessage message = ZmqMessage.builder()
                                   .withTopic(topic)
                                   .withIdentities(identities)
                                   .withHeaders(headers)
                                   .withPayload(payload)
                                   .build();

    assertEq(topic, message.topic());
    assertEquals(identities.size(), message.identityFrames().size());
    assertArrayEquals(headers, message.headersAsBinary());
    assertEq(payload, message.payload());

    assertEq(message, ZmqMessage.builder(message).build());
  }

  @Test
  public void t2() {
    ZmqMessage message = ZmqMessage.builder()
                                   .withTopic(topic)
                                   .withIdentities(identities)
                                   .withHeaders(headers)
                                   .withPayload(payload)
                                   .build();

    message = ZmqMessage.builder(message)
                        .withHeaders(new ZmqHeaders()
                                         .copy(message.headersAsBinary())
                                         .set("a", "xyz")
                                         .set("0", headerOverride0)
                                         .set("1", headerOverride1))
                        .build();

    assertEq(topic, message.topic());
    assertEquals(identities.size(), message.identityFrames().size());
    assertArrayEquals("{\"a\":[\"xyz\"],\"0\":[\"h0\"],\"1\":[\"h1\"]}".getBytes(), message.headersAsBinary());
    assertEq(payload, message.payload());

    assertEq(message, ZmqMessage.builder(message).build());
  }

  @Test
  public void t3() {
    ZmqMessage message = ZmqMessage.builder().build();

    assertEq(EMPTY_FRAME, message.topic());
    assertEq(EMPTY_FRAME, message.payload());
    assertEquals(0, message.identityFrames().size());
    assertArrayEquals(EMPTY_FRAME, message.headersAsBinary());
  }

  @Test
  public void t4() {
    ZmqMessage message = ZmqMessage.builder().build();

    ZmqMessage copyMessage = ZmqMessage.builder(message)
                                       .withTopic(topic)
                                       .withIdentities(identities)
                                       .withHeaders(headers)
                                       .withPayload(payload)
                                       .build();

    assertEq(topic, copyMessage.topic());
    assertEq(payload, copyMessage.payload());
    assertEquals(3, copyMessage.identityFrames().size());
    assertArrayEquals(headers, copyMessage.headersAsBinary());
  }

  @Test
  public void t5() {
    ZmqMessage message = ZmqMessage.builder()
                                   .withTopic(topic)
                                   .withTopic(topic)
                                   .withIdentities(identities)
                                   .withIdentities(identities)
                                   .withIdentities(identities)
                                   .withIdentities(identities)
                                   .withIdentities(identities)
                                   .withIdentities(identities)
                                   .withHeaders(headers)
                                   .withHeaders(headers)
                                   .withPayload(payload)
                                   .withPayload(payload)
                                   .build();

    assertEq(topic, message.topic());
    assertEquals(identities.size(), message.identityFrames().size());
    assertArrayEquals(headers, message.headersAsBinary());
    assertEq(payload, message.payload());
  }

  @Test
  public void t6() {
    // check topic
    try {
      ZmqMessage.builder()
                .withTopic(DIV_FRAME)
                .build();
      fail();
    }
    catch (IllegalArgumentException e) {
    }
    try {
      ZmqMessage.builder()
                .withTopic(null)
                .build();
      fail();
    }
    catch (IllegalArgumentException e) {
    }
    // check payload
    try {
      ZmqMessage.builder()
                .withPayload(DIV_FRAME)
                .build();
      fail();
    }
    catch (IllegalArgumentException e) {
    }
    try {
      ZmqMessage.builder()
                .withPayload(null)
                .build();
      fail();
    }
    catch (IllegalArgumentException e) {
    }
    // check identities
    try {
      ZmqFrames i = new ZmqFrames();
      i.add(DIV_FRAME);
      ZmqMessage.builder()
                .withIdentities(i)
                .build();
      fail();
    }
    catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void t8() {
    ZmqMessage src = ZmqMessage.builder()
                               .withHeaders(new ZmqHeaders().set("a", "b"))
                               .build();

    ZmqMessage copy = ZmqMessage.builder(src)
                                .withHeaders(new ZmqHeaders().set("x", "x"))
                                .build();

    assertArrayEquals("{\"x\":[\"x\"]}".getBytes(), copy.headersAsBinary());

    ZmqMessage copy2 = ZmqMessage.builder(src)
                                 .withHeaders(new ZmqHeaders()
                                                  .copy(src.headers())
                                                  .set("y", "y"))
                                 .build();

    assertArrayEquals("{\"a\":[\"b\"],\"y\":[\"y\"]}".getBytes(), copy2.headersAsBinary());
  }

  private void assertEq(byte[] a, byte[] b) {
    assert Arrays.equals(a, b);
  }

  private void assertEq(ZmqMessage a, ZmqMessage b) {
    assert a != b;
    assertEq(a.topic(), b.topic());
    assertEquals(a.identityFrames().size(), b.identityFrames().size());
    assertArrayEquals(a.headersAsBinary(), b.headersAsBinary());
    assertEq(a.payload(), b.payload());
  }
}
