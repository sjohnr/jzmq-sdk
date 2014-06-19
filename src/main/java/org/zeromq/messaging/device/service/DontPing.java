package org.zeromq.messaging.device.service;

import org.zeromq.messaging.ZmqChannel;

class DontPing implements ZmqPingStrategy {

  @Override
  public void ping(ZmqChannel channel) {
    // no-op.
  }
}
