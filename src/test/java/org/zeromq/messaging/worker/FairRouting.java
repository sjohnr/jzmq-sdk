package org.zeromq.messaging.worker;

import org.zeromq.messaging.service.Routing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.zeromq.support.ZmqUtils.makeHash;

class FairRouting implements Routing {

  private Map<Long, byte[]> routing = new HashMap<>();
  private AtomicLong fairCounter = new AtomicLong();

  FairRouting() {
  }

  @Override
  public void put(byte[] identity, byte[] payload) {
    routing.put(makeHash(identity), identity);
  }

  @Override
  public byte[] get(byte[] identity, byte[] payload) {
    List<Long> routingKeyList = new ArrayList<>(routing.keySet());
    int i = (int) (fairCounter.incrementAndGet() % routingKeyList.size());
    return routing.get(routingKeyList.get(i));
  }

  @Override
  public int available() {
    return routing.size();
  }
}
