package org.zeromq.messaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import static com.google.common.base.Preconditions.checkArgument;
import static org.zeromq.support.ZmqUtils.isEmptyFrame;

public final class ZmqFrames extends ArrayList<byte[]> {

  public static final byte[] EMPTY_FRAME = "".getBytes();
  public static final byte BYTE_SUB = 1; // denotes subscribe request.
  public static final byte BYTE_UNSUB = 0; // denotes unsubscribe request.

  public ZmqFrames(int initialCapacity) {
    super(initialCapacity);
  }

  public ZmqFrames() {
  }

  public ZmqFrames(Collection<? extends byte[]> c) {
    super(c);
  }

  public ZmqFrames(byte[] frame) {
    super(1);
    add(frame);
  }

  public ZmqFrames(byte[] frame0, byte[] frame1) {
    super(2);
    add(frame0);
    add(frame1);
  }

  public ZmqFrames(byte[] frame0, byte[] frame1, byte[] frame2) {
    super(3);
    add(frame0);
    add(frame1);
    add(frame2);
  }

  public byte getExtPubSub() {
    checkArgument(size() == 1, "Wrong frames.size: " + size());
    byte b = get(0)[0];
    checkArgument(BYTE_SUB == b || BYTE_UNSUB == b, "Unrecognized ExtPubSub byte: " + b);
    return b;
  }

  public byte[] getExtPubSubTopic() {
    checkArgument(size() == 1, "Wrong frames.size: " + size());
    byte[] buf = get(0);
    return buf.length > 1 ? Arrays.copyOfRange(buf, 1, buf.length) : EMPTY_FRAME;
  }

  public byte[] getTopic() {
    checkArgument(size() > 0, "Wrong frames.size: " + size());
    return get(0);
  }

  public ZmqFrames getIdentities() {
    ZmqFrames identities = new ZmqFrames();
    int emptyFrameSeen = 0;
    for (int i = 0; i < size(); i++) {
      byte[] frame = get(i);
      if (isEmptyFrame(frame)) {
        ++emptyFrameSeen;
      }
      if (emptyFrameSeen == 2) {
        break;
      }
      if (!isEmptyFrame(frame)) {
        emptyFrameSeen = 0;
        identities.add(frame);
      }
    }
    return identities;
  }

  public byte[] getPayload() {
    checkArgument(size() > 0, "Wrong frames.size: " + size());
    ByteBuffer buf = ByteBuffer.wrap(get(size() - 1));
    byte[] payload = new byte[buf.getInt()];
    buf.get(payload);
    return payload;
  }

  public int getInprocRef() {
    checkArgument(size() > 0, "Wrong frames.size: " + size());
    byte[] buf = get(size() - 1);
    checkArgument(buf.length == 4);
    return ((buf[0] & 0xFF) << 24) | ((buf[1] & 0xFF) << 16) | ((buf[2] & 0xFF) << 8) | buf[3] & 0xFF;
  }
}
