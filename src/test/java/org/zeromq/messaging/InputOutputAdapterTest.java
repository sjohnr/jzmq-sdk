package org.zeromq.messaging;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.zeromq.messaging.ZmqMessage.BYTE_SUB;
import static org.zeromq.messaging.ZmqMessage.BYTE_UNSUB;
import static org.zeromq.messaging.ZmqMessage.EMPTY_FRAME;
import static org.zeromq.support.ZmqUtils.mergeBytes;

public class InputOutputAdapterTest {

  static final Logger LOG = LoggerFactory.getLogger(InputOutputAdapterTest.class);

  private static final byte[] id_0 = "i0".getBytes();
  private static final byte[] id_1 = "i1".getBytes();
  private static final byte[] id_2 = "i2".getBytes();

  private static final byte[] topic = "topic".getBytes();
  private static final byte[] headers = "h0=h0,h1=h1,h2=h2".getBytes();
  private static final byte[] payload = "payload".getBytes();

  private static final ZmqFrames identities = new ZmqFrames();

  static {
    identities.add(id_0);
    identities.add(id_1);
    identities.add(id_2);
  }

  private static final byte[] headers_payload =
      ByteBuffer.allocate(4 + 17 + 4 + 7)
                .putInt(17)
                .put("h0=h0,h1=h1,h2=h2".getBytes())
                .putInt(7)
                .put("payload".getBytes())
                .array();

  @Test
  public void t0() {
    LOG.info(
        "\n" +
        "Test conversion:                                                \n" +
        "                                                                \n" +
        "[TOPIC | [HEADER] | PAYLOAD]  ==>  [FRAME, ..., FRAME]          \n" +
        "[FRAME, ..., FRAME]           ==>  [TOPIC | [HEADER] | PAYLOAD] \n" +
        "                                                                \n");

    ZmqMessage message = ZmqMessage.builder()
                                   .withTopic(topic)
                                   .withHeaders(headers)
                                   .withPayload(payload)
                                   .build();

    OutputAdapter out = OutputAdapter.builder()
                                     .awareOfTopicFrame()
                                     .build();
    ZmqFrames frames = out.convert(message);
    Iterator<byte[]> framesIter = frames.iterator();

    assertNextFrame(topic, framesIter);
    assertNextFrame(headers_payload, framesIter);

    assert !framesIter.hasNext();

    InputAdapter input = InputAdapter.builder()
                                     .awareOfTopicFrame()
                                     .build();
    assertMessage(message, input.convert(frames));
  }

  @Test
  public void t1() {
    LOG.info(
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

    OutputAdapter out = OutputAdapter.builder().build();
    ZmqFrames frames = out.convert(message);
    Iterator<byte[]> framesIter = frames.iterator();

    assertNextFrame(headers_payload, framesIter);

    assert !framesIter.hasNext();

    InputAdapter input = InputAdapter.builder().build();
    assertMessage(message, input.convert(frames));
  }

  @Test
  public void t2() {
    LOG.info(
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

    OutputAdapter output = OutputAdapter.builder()
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
    assertNextFrame(EMPTY_FRAME, framesIter);
    assertNextFrame(headers_payload, framesIter);

    assert !framesIter.hasNext();

    InputAdapter input = InputAdapter.builder()
                                     .expectIdentities()
                                     .build();
    assertMessage(message, input.convert(frames));
  }

  @Test
  public void t3() {
    LOG.info(
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

    OutputAdapter output = OutputAdapter.builder()
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
    assertNextFrame(EMPTY_FRAME, framesIter);
    assertNextFrame(headers_payload, framesIter);

    assert !framesIter.hasNext();

    InputAdapter input = InputAdapter.builder()
                                     .expectIdentities()
                                     .build();
    assertMessage(message, input.convert(frames));
  }

  @Test
  public void t4() {
    ZmqMessage message = ZmqMessage.builder()
                                   .withTopic(topic)
                                   .withExtendedPubSubFlag(BYTE_SUB)
                                   .build();

    OutputAdapter output = OutputAdapter.builder()
                                        .awareOfTopicFrame()
                                        .awareOfExtendedPubSub()
                                        .build();

    ZmqFrames frames = output.convert(message);
    Iterator<byte[]> framesIter = frames.iterator();

    assertNextFrame(mergeBytes(ImmutableList.of(new byte[]{BYTE_SUB}, topic)), framesIter);
    assert !framesIter.hasNext();

    InputAdapter input = InputAdapter.builder()
                                     .awareOfTopicFrame()
                                     .awareOfExtendedPubSub()
                                     .build();

    assertMessage(message, input.convert(frames));
  }

  @Test
  public void t5() {
    ZmqMessage message = ZmqMessage.builder()
                                   .withTopic(EMPTY_FRAME)
                                   .withExtendedPubSubFlag(BYTE_UNSUB)
                                   .build();

    OutputAdapter output = OutputAdapter.builder()
                                        .awareOfTopicFrame()
                                        .awareOfExtendedPubSub()
                                        .build();

    ZmqFrames frames = output.convert(message);
    Iterator<byte[]> framesIter = frames.iterator();

    assertNextFrame(mergeBytes(ImmutableList.of(new byte[]{BYTE_UNSUB}, EMPTY_FRAME)), framesIter);
    assert !framesIter.hasNext();

    InputAdapter input = InputAdapter.builder()
                                     .awareOfTopicFrame()
                                     .awareOfExtendedPubSub()
                                     .build();

    assertMessage(message, input.convert(frames));
  }

  private void assertNextFrame(byte[] frame, Iterator<byte[]> it) {
    assertTrue(Arrays.equals(frame, it.next()));
  }

  private void assertEq(byte[] a, byte[] b) {
    assert Arrays.equals(a, b);
  }

  private void assertMessage(ZmqMessage a, ZmqMessage b) {
    assert a != b;
    assertEq(a.topic(), b.topic());
    assertEquals(a.extendedPubSubFlag(), b.extendedPubSubFlag());
    assertEq(a.payload(), b.payload());

    List<byte[]> a_identities = new ArrayList<byte[]>(a.identityFrames());
    List<byte[]> b_identities = new ArrayList<byte[]>(b.identityFrames());
    assertEquals(a_identities.size(), b_identities.size());
    for (int i = 0; i < a_identities.size(); i++) {
      assertEq(a_identities.get(i), b_identities.get(i));
    }

    assertArrayEquals(a.headers(), b.headers());
  }
}
