package org.zeromq.messaging.device.service;

import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqHeaders;
import org.zeromq.messaging.ZmqMessage;

import static org.zeromq.messaging.ZmqCommonHeaders.Header.ZMQ_MESSAGE_TYPE;
import static org.zeromq.messaging.ZmqCommonHeaders.ZMQ_MESSAGE_TYPE_PING;

class DoPing implements ZmqPingStrategy {

  private static final ZmqMessage PING;

  static {
    PING = ZmqMessage.builder()
                     .withHeaders(ZmqHeaders.builder()
                                            .set(ZMQ_MESSAGE_TYPE.id(), ZMQ_MESSAGE_TYPE_PING)
                                            .build()
                                            .asBinary())
                     .build();
  }

  @Override
  public void ping(ZmqChannel channel) {
    channel.send(PING);
  }
}
