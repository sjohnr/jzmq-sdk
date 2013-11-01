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
import org.zeromq.TestRecorder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.zeromq.messaging.ZmqMessage.DIV_FRAME;
import static org.zeromq.messaging.ZmqMessage.EMPTY_FRAME;
import static org.zeromq.support.ZmqUtils.intAsBytes;
import static org.zeromq.support.ZmqUtils.mergeBytes;

public class InputOutputMessageAdapterTest {

  private static final byte[] id_0 = "i0".getBytes();
  private static final byte[] id_1 = "i1".getBytes();
  private static final byte[] id_2 = "i2".getBytes();

  private static final byte[] h_0 = mergeBytes(Arrays.asList(intAsBytes(0), "h0".getBytes()));
  private static final byte[] h_1 = mergeBytes(Arrays.asList(intAsBytes(1), "h1".getBytes()));
  private static final byte[] h_2 = mergeBytes(Arrays.asList(intAsBytes(2), "h2".getBytes()));

  private static final byte[] payload = "payload".getBytes();
  private static final byte[] topic = "topic".getBytes();

  private static final ZmqFrames identities = new ZmqFrames();

  static {
    identities.add(id_0);
    identities.add(id_1);
    identities.add(id_2);
  }

  private static final ZmqFrames headers = new ZmqFrames();

  static {
    headers.add(h_0);
    headers.add(h_1);
    headers.add(h_2);
  }

  @Test
  public void t0() {
    TestRecorder r = new TestRecorder().start();
    r.log(
        "\n" +
        "Test conversion:                                                \n" +
        "                                                                \n" +
        "[TOPIC | [HEADER] | PAYLOAD]  ==>  [FRAME, ..., FRAME]          \n" +
        "[FRAME, ..., FRAME]           ==>  [TOPIC | [HEADER] | PAYLOAD] \n" +
        "                                                                \n" +
        "                                                                \n");

    ZmqMessage message = ZmqMessage.builder()
                                   .withTopic(topic)
                                   .withHeaders(headers)
                                   .withPayload(payload)
                                   .build();

    OutputMessageAdapter out = OutputMessageAdapter.builder()
                                                   .awareOfTopicFrame()
                                                   .build();
    ZmqFrames frames = out.convert(message);
    Iterator<byte[]> framesIter = frames.iterator();

    assertNextFrame(topic, framesIter);
    assertNextFrame(DIV_FRAME, framesIter);
    assertNextFrame(h_0, framesIter);
    assertNextFrame(h_1, framesIter);
    assertNextFrame(h_2, framesIter);
    assertNextFrame(DIV_FRAME, framesIter);
    assertNextFrame(payload, framesIter);

    assert !framesIter.hasNext();

    InputMessageAdapter input = InputMessageAdapter.builder()
                                                   .awareOfTopicFrame()
                                                   .build();
    assertEq(message, input.convert(frames));
  }

  @Test
  public void t1() {
    TestRecorder r = new TestRecorder().start();
    r.log(
        "\n" +
        "Test conversion:                               \n" +
        "                                               \n" +
        "[[HEADER] | PAYLOAD]  =>  [FRAME, ..., FRAME]  \n" +
        "[FRAME, ..., FRAME])  =>  [[HEADER] | PAYLOAD] \n" +
        "                                               \n" +
        "                                               \n");

    ZmqMessage message = ZmqMessage.builder()
                                   .withHeaders(headers)
                                   .withPayload(payload)
                                   .build();

    OutputMessageAdapter out = OutputMessageAdapter.builder()
                                                   .awareOfTopicFrame()
                                                   .build();
    ZmqFrames frames = out.convert(message);
    Iterator<byte[]> framesIter = frames.iterator();

    assertNextFrame(EMPTY_FRAME, framesIter);
    assertNextFrame(DIV_FRAME, framesIter);
    assertNextFrame(h_0, framesIter);
    assertNextFrame(h_1, framesIter);
    assertNextFrame(h_2, framesIter);
    assertNextFrame(DIV_FRAME, framesIter);
    assertNextFrame(payload, framesIter);

    assert !framesIter.hasNext();

    InputMessageAdapter input = InputMessageAdapter.builder()
                                                   .awareOfTopicFrame()
                                                   .build();
    assertEq(message, input.convert(frames));
  }

  @Test
  public void t2() {
    TestRecorder r = new TestRecorder().start();
    r.log(
        "\n" +
        "Test conversion:                                                                   \n" +
        "                                                                                   \n" +
        "[[PEER_IDENTITY] | [HEADER] | PAYLOAD]  =>  [FRAME, ..., FRAME]                    \n" +
        "[FRAME, ..., FRAME]                     =>  [[PEER_IDENTITY] | [HEADER] | PAYLOAD] \n" +
        "                                                                                   \n" +
        "                                                                                   \n");

    ZmqMessage message = ZmqMessage.builder()
                                   .withIdentities(identities)
                                   .withHeaders(headers)
                                   .withPayload(payload)
                                   .build();

    OutputMessageAdapter output = OutputMessageAdapter.builder()
                                                      .expectIdentities()
                                                      .build();
    ZmqFrames frames = output.convert(message);
    Iterator<byte[]> framesIter = frames.iterator();

    assertNextFrame(id_0, framesIter);
    assertNextFrame(EMPTY_FRAME, framesIter);
    assertNextFrame(id_1, framesIter);
    assertNextFrame(EMPTY_FRAME, framesIter);
    assertNextFrame(id_2, framesIter);
    assertNextFrame(EMPTY_FRAME, framesIter);
    assertNextFrame(DIV_FRAME, framesIter);
    assertNextFrame(h_0, framesIter);
    assertNextFrame(h_1, framesIter);
    assertNextFrame(h_2, framesIter);
    assertNextFrame(DIV_FRAME, framesIter);
    assertNextFrame(payload, framesIter);

    assert !framesIter.hasNext();

    InputMessageAdapter input = InputMessageAdapter.builder()
                                                   .expectIdentities()
                                                   .build();
    assertEq(message, input.convert(frames));
  }

  @Test
  public void t3() {
    TestRecorder r = new TestRecorder().start();
    r.log(
        "\n" +
        "Test conversion (DEALER case):                                                     \n" +
        "                                                                                   \n" +
        "[[PEER_IDENTITY] | [HEADER] | PAYLOAD]  =>  [FRAME, ..., FRAME]                    \n" +
        "[FRAME, ..., FRAME])                    =>  [[PEER_IDENTITY] | [HEADER] | PAYLOAD] \n" +
        "                                                                                   \n" +
        "                                                                                   \n");

    ZmqMessage message = ZmqMessage.builder()
                                   .withIdentities(identities)
                                   .withHeaders(headers)
                                   .withPayload(payload)
                                   .build();

    OutputMessageAdapter output = OutputMessageAdapter.builder()
                                                      .awareOfDEALERType()
                                                      .expectIdentities()
                                                      .build();
    ZmqFrames frames = output.convert(message);
    Iterator<byte[]> framesIter = frames.iterator();

    assertNextFrame(EMPTY_FRAME, framesIter);
    assertNextFrame(id_0, framesIter);
    assertNextFrame(EMPTY_FRAME, framesIter);
    assertNextFrame(id_1, framesIter);
    assertNextFrame(EMPTY_FRAME, framesIter);
    assertNextFrame(id_2, framesIter);
    assertNextFrame(EMPTY_FRAME, framesIter);
    assertNextFrame(DIV_FRAME, framesIter);
    assertNextFrame(h_0, framesIter);
    assertNextFrame(h_1, framesIter);
    assertNextFrame(h_2, framesIter);
    assertNextFrame(DIV_FRAME, framesIter);
    assertNextFrame(payload, framesIter);

    assert !framesIter.hasNext();

    InputMessageAdapter input = InputMessageAdapter.builder()
                                                   .expectIdentities()
                                                   .build();
    assertEq(message, input.convert(frames));
  }

  @Test
  public void t4() {
    TestRecorder r = new TestRecorder().start();
    r.log(
        "\n" +
        "Test conversion:                                                                                   \n" +
        "                                                                                                   \n" +
        "[TOPIC | [PEER_IDENTITY] | [HEADER] | PAYLOAD]  =>  [FRAME, ..., FRAME]                            \n" +
        "[FRAME, ..., FRAME]                             =>  [TOPIC | [PEER_IDENTITY] | [HEADER] | PAYLOAD] \n" +
        "                                                                                                   \n" +
        "                                                                                                   \n");

    ZmqMessage message = ZmqMessage.builder()
                                   .withTopic(topic)
                                   .withIdentities(identities)
                                   .withHeaders(headers)
                                   .withPayload(payload)
                                   .build();

    OutputMessageAdapter output = OutputMessageAdapter.builder()
                                                      .awareOfTopicFrame()
                                                      .expectIdentities()
                                                      .build();
    ZmqFrames frames = output.convert(message);
    Iterator<byte[]> framesIter = frames.iterator();

    assertNextFrame(topic, framesIter);
    assertNextFrame(DIV_FRAME, framesIter);
    assertNextFrame(id_0, framesIter);
    assertNextFrame(EMPTY_FRAME, framesIter);
    assertNextFrame(id_1, framesIter);
    assertNextFrame(EMPTY_FRAME, framesIter);
    assertNextFrame(id_2, framesIter);
    assertNextFrame(EMPTY_FRAME, framesIter);
    assertNextFrame(DIV_FRAME, framesIter);
    assertNextFrame(h_0, framesIter);
    assertNextFrame(h_1, framesIter);
    assertNextFrame(h_2, framesIter);
    assertNextFrame(DIV_FRAME, framesIter);
    assertNextFrame(payload, framesIter);

    assert !framesIter.hasNext();

    InputMessageAdapter input = InputMessageAdapter.builder()
                                                   .awareOfTopicFrame()
                                                   .expectIdentities()
                                                   .build();
    assertEq(message, input.convert(frames));
  }

  private void assertNextFrame(byte[] frame, Iterator<byte[]> it) {
    assertTrue(Arrays.equals(frame, it.next()));
  }

  private void assertEq(byte[] a, byte[] b) {
    assert Arrays.equals(a, b);
  }

  private void assertEq(ZmqMessage a, ZmqMessage b) {
    assert a != b;
    assertEq(a.topic(), b.topic());
    assertEq(a.payload(), b.payload());

    List<byte[]> a_identities = new ArrayList<byte[]>(a.identities());
    List<byte[]> b_identities = new ArrayList<byte[]>(b.identities());
    assertEquals(a_identities.size(), b_identities.size());
    for (int i = 0; i < a_identities.size(); i++) {
      assertEq(a_identities.get(i), b_identities.get(i));
    }
    ArrayList<byte[]> a_headers = new ArrayList<byte[]>(a.headers());
    ArrayList<byte[]> b_headers = new ArrayList<byte[]>(b.headers());
    assertEquals(a_headers.size(), b_headers.size());
    for (int i = 0; i < a_headers.size(); i++) {
      assertEq(a_headers.get(i), b_headers.get(i));
    }
  }
}