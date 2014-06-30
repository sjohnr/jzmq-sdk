package org.zeromq.messaging.worker;

import com.google.common.collect.ImmutableList;
import org.zeromq.messaging.ZmqFrames;
import org.zeromq.messaging.service.ZmqRouting;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.zeromq.support.ZmqUtils.makeHash;
import static org.zeromq.support.ZmqUtils.mergeBytes;

class FairRouting implements ZmqRouting {

  private Map<Long, ZmqFrames> routing = new HashMap<>();
  private AtomicLong fairCounter = new AtomicLong();

  FairRouting() {
  }

  @Override
  public void putRouting(ZmqFrames route) {
    long routingKey = makeHash(ImmutableList.of(mergeBytes(route)));
    routing.put(routingKey, new ZmqFrames(route));
  }

  @Override
  public ZmqFrames getRouting(ZmqFrames route) {
    List<Long> routingKeyList = new ArrayList<>(routing.keySet());
    int i = (int) (fairCounter.incrementAndGet() % routingKeyList.size());
    Long routingKey = routingKeyList.get(i);
    return new ZmqFrames(routing.get(routingKey));
  }

  @Override
  public int available() {
    return routing.size();
  }
}
