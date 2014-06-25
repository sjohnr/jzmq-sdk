package org.zeromq.messaging;

import java.util.ArrayList;
import java.util.Collection;

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
}
