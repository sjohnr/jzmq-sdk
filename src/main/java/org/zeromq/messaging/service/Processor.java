package org.zeromq.messaging.service;

import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqFrames;

public interface Processor<T extends Processor> {

  T set(ZmqFrames route);

  T set(byte[] payload);

  T set(ZmqChannel router);

  T set(Routing[] routings);

  T set(Object[] identities);

  void onRoot() throws Exception;

  void onMaster() throws Exception;

  void onSlave() throws Exception;
}
