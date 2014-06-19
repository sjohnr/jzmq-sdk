package org.zeromq.messaging;

import java.util.ArrayDeque;
import java.util.Collection;

public final class ZmqFrames extends ArrayDeque<byte[]> {

  public ZmqFrames(int initialCapacity) {
    super(initialCapacity);
  }

  public ZmqFrames() {
  }

  public ZmqFrames(Collection<? extends byte[]> c) {
    super(c);
  }
}
