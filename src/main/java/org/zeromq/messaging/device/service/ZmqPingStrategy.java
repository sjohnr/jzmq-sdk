package org.zeromq.messaging.device.service;

import org.zeromq.messaging.ZmqChannel;

public interface ZmqPingStrategy {

  void ping(ZmqChannel channel);
}
