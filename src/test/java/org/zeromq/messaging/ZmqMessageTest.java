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

import com.google.common.base.Stopwatch;
import org.junit.Test;
import org.zeromq.TestRecorder;

import java.util.Arrays;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.zeromq.messaging.ZmqAbstractTest.Fixture.HELLO;
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
    assertEquals(0, message.identities().size());
    assertArrayEquals(EMPTY_FRAME, message.headers());
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
    assertEquals(identities.size(), message.identities().size());
    assertArrayEquals(headers, message.headers());
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
                                         .copy(message.headersAs(ZmqHeaders.class))
                                         .set("a", "xyz")
                                         .set("0", headerOverride0)
                                         .set("1", headerOverride1))
                        .build();

    assertEq(topic, message.topic());
    assertEquals(identities.size(), message.identities().size());
    assertArrayEquals("{\"a\":[\"xyz\"],\"0\":[\"h0\"],\"1\":[\"h1\"]}".getBytes(), message.headers());
    assertEq(payload, message.payload());

    assertEq(message, ZmqMessage.builder(message).build());
  }

  @Test
  public void t3() {
    ZmqMessage message = ZmqMessage.builder().build();

    assertEq("".getBytes(), message.topic());
    assertEq("".getBytes(), message.payload());
    assertEquals(0, message.identities().size());
    assertArrayEquals(EMPTY_FRAME, message.headers());
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
    assertEquals(3, copyMessage.identities().size());
    assertArrayEquals(headers, copyMessage.headers());
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
    assertEquals(6 * identities.size(), message.identities().size());
    assertArrayEquals(headers, message.headers());
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
  public void t7_perf() {
    TestRecorder r = new TestRecorder();
    r.log("Perf test for ZmqMessage. Test what it takes to create-and-copy million simple messages.");
    Stopwatch timer = new Stopwatch().start();
    int ITER = 10;
    int MESSAGE_NUM = 100000;
    for (int j = 0; j < ITER; j++) {
      for (int i = 0; i < MESSAGE_NUM; i++) {
        ZmqMessage.builder(HELLO()).withHeaders(new ZmqHeaders().set("x", "y")).build();
      }
    }
    r.logQoS((float) timer.stop().elapsedTime(MICROSECONDS) / (ITER * MESSAGE_NUM), "microsec/message.");
  }

  private void assertEq(byte[] a, byte[] b) {
    assert Arrays.equals(a, b);
  }

  private void assertEq(ZmqMessage a, ZmqMessage b) {
    assert a != b;
    assertEq(a.topic(), b.topic());
    assertEquals(a.identities().size(), b.identities().size());
    assertArrayEquals(a.headers(), b.headers());
    assertEq(a.payload(), b.payload());
  }
}
