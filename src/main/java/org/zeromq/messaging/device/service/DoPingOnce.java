package org.zeromq.messaging.device.service;

import org.zeromq.messaging.ZmqChannel;

class DoPingOnce extends DoPing {

  private int _counter;

  @Override
  public void ping(ZmqChannel channel) {
    if (_counter == 0) {
      super.ping(channel);
      _counter++;
    }
  }
}
