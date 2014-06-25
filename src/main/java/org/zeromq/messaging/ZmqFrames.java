package org.zeromq.messaging;

import java.util.ArrayList;
import java.util.Collection;

public final class ZmqFrames extends ArrayList<byte[]> {

  public ZmqFrames(int initialCapacity) {
    super(initialCapacity);
  }

  public ZmqFrames() {
  }

  public ZmqFrames(Collection<? extends byte[]> c) {
    super(c);
  }
}
