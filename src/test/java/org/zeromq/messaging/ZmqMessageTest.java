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
import org.zeromq.messaging.device.CallerHeaders;

import java.util.ArrayList;
import java.util.Arrays;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.zeromq.messaging.ZmqAbstractTest.Fixture.HELLO;
import static org.zeromq.messaging.ZmqMessage.DIV_FRAME;
import static org.zeromq.support.ZmqUtils.intAsBytes;
import static org.zeromq.support.ZmqUtils.mergeBytes;

public class ZmqMessageTest {

  private static final byte[] id0 = "id0".getBytes();
  private static final byte[] id1 = "id1".getBytes();
  private static final byte[] id2 = "id2".getBytes();

  private static final byte[] header0 = mergeBytes(Arrays.asList(intAsBytes(0), "header0".getBytes()));
  private static final byte[] header1 = mergeBytes(Arrays.asList(intAsBytes(1), "header1".getBytes()));
  private static final byte[] header2 = mergeBytes(Arrays.asList(intAsBytes(2), "header2".getBytes()));
  private static final byte[] header3 = mergeBytes(Arrays.asList(intAsBytes(3), "header3".getBytes()));
  private static final byte[] header4 = mergeBytes(Arrays.asList(intAsBytes(4), "header4".getBytes()));

  private static final byte[] headerOverride0 = "x".getBytes();
  private static final byte[] headerOverride1 = "y".getBytes();

  private static final byte[] topic = "topic".getBytes();
  private static final byte[] payload = "payload".getBytes();
  private static final ZmqFrames identities = new ZmqFrames();

  static {
    identities.add(id0);
    identities.add(id1);
    identities.add(id2);
  }

  private static final ZmqFrames headers = new ZmqFrames();

  static {
    headers.add(header0);
    headers.add(header1);
    headers.add(header2);
    headers.add(header3);
    headers.add(header4);
  }

  @Test
  public void t0() {
    ZmqMessage message = ZmqMessage.builder()
                                   .withTopic(topic)
                                   .withPayload(payload)
                                   .build();

    assertEq(topic, message.topic());
    assertEq(payload, message.payload());
    assertEquals(0, message.identities().size());
    assertEquals(0, message.headers().size());

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
    assertEq(payload, message.payload());
    assertEquals(identities.size(), message.identities().size());
    assertEquals(headers.size(), message.headers().size());

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
                                         .copyOf(message.headersAs(ZmqHeaders.class))
                                         .put(0, headerOverride0)
                                         .put(1, headerOverride1))
                        .build();

    assertEq(topic, message.topic());
    assertEq(payload, message.payload());
    assertEquals(identities.size(), message.identities().size());
    assertEquals(headers.size(), message.headers().size());

    assertEq(headerOverride0, message.headersAs(ZmqHeaders.class).getHeaderOrNull(0));
    assertEq(headerOverride1, message.headersAs(ZmqHeaders.class).getHeaderOrNull(1));

    assertEq(message, ZmqMessage.builder(message).build());
  }

  @Test
  public void t3() {
    ZmqMessage message = ZmqMessage.builder()
                                   .withTopic(topic)
                                   .withIdentities(identities)
                                   .withHeaders(headers)
                                   .withPayload(payload)
                                   .build();

    ZmqMessage copyMessage = ZmqMessage.builder(message)
                                       .withHeaders(new ZmqHeaders()
                                                        .copyOf(message.headersAs(ZmqHeaders.class))
                                                        .put(0, headerOverride0)
                                                        .put(1, headerOverride1))
                                       .build();

    assertEq(message, copyMessage);

    assertEq(headerOverride0, copyMessage.headersAs(ZmqHeaders.class).getHeaderOrNull(0));
    assertEq(headerOverride1, copyMessage.headersAs(ZmqHeaders.class).getHeaderOrNull(1));
  }

  @Test
  public void t4() {
    ZmqMessage message = ZmqMessage.builder().build();

    assertEq("".getBytes(), message.topic());
    assertEq("".getBytes(), message.payload());
    assertEquals(0, message.identities().size());
    assertEquals(0, message.headers().size());
  }

  @Test
  public void t5() {
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
    assertEquals(5, copyMessage.headers().size());
  }

  @Test
  public void t6() {
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
    assertEq(payload, message.payload());
    assertEquals(6 * identities.size(), message.identities().size());
    assertEquals(headers.size(), message.headers().size());
  }

  @Test
  public void t7() {
    // check topic
    try {
      ZmqMessage.builder()
                .withTopic(DIV_FRAME)
                .build();
      fail();
    }
    catch (AssertionError e) {
    }
    try {
      ZmqMessage.builder()
                .withTopic(null)
                .build();
      fail();
    }
    catch (AssertionError e) {
    }
    // check payload
    try {
      ZmqMessage.builder()
                .withPayload(DIV_FRAME)
                .build();
      fail();
    }
    catch (AssertionError e) {
    }
    try {
      ZmqMessage.builder()
                .withPayload(null)
                .build();
      fail();
    }
    catch (AssertionError e) {
    }
    // check headers
    try {
      ZmqFrames h = new ZmqFrames();
      h.add(DIV_FRAME);
      ZmqMessage.builder()
                .withHeaders(h)
                .build();
      fail();
    }
    catch (AssertionError e) {
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
    catch (AssertionError e) {
    }
  }

  @Test
  public void t8() {
    ZmqMessage message = ZmqMessage.builder()
                                   .withTopic(topic)
                                   .withIdentities(identities)
                                   .withHeaders(headers)
                                   .withPayload(payload)
                                   .build();

    ZmqMessage copyMessage = ZmqMessage.builder(message)
                                       .withHeaders(new ZmqHeaders()
                                                        .copyOf(message.headersAs(ZmqHeaders.class))
                                                        .put(0, headerOverride0))
                                       .build();

    assertEq(message, copyMessage);
    assertEq(headerOverride0, copyMessage.headersAs(ZmqHeaders.class).getHeaderOrNull(0));

    ZmqMessage anotherCopyMessage = ZmqMessage.builder(copyMessage)
                                              .withHeaders(new ZmqHeaders()
                                                               .copyOf(copyMessage.headersAs(ZmqHeaders.class))
                                                               .put(1, headerOverride1))
                                              .build();

    assertEq(message, anotherCopyMessage);
    assertEq(headerOverride0, copyMessage.headersAs(ZmqHeaders.class).getHeaderOrNull(0));
    assertEq(headerOverride1, anotherCopyMessage.headersAs(ZmqHeaders.class).getHeaderOrNull(1));
  }

  @Test
  public void t9_perf() {
    TestRecorder r = new TestRecorder();
    r.log("Perf test for ZmqMessage. Test what it takes to create-and-copy million simple messages.");
    Stopwatch timer = new Stopwatch().start();
    int ITER = 10;
    int MESSAGE_NUM = 100000;
    for (int j = 0; j < ITER; j++) {
      for (int i = 0; i < MESSAGE_NUM; i++) {
        ZmqMessage.builder(HELLO()).withHeaders(new CallerHeaders().setMsgTypeTryAgain()).build();
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
    assertEq(a.payload(), b.payload());
    assertEquals(a.identities().size(), b.identities().size());
    ArrayList<byte[]> a_headers = new ArrayList<byte[]>(a.headers());
    ArrayList<byte[]> b_headers = new ArrayList<byte[]>(b.headers());
    assertEquals(a_headers.size(), b_headers.size());
  }
}
