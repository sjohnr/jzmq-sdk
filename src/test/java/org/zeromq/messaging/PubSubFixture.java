package org.zeromq.messaging;

import static org.zeromq.messaging.ZmqAbstractTest.inproc;

class PubSubFixture extends BaseFixture {

  PubSubFixture(ZmqContext ctx) {
    super(ctx);
  }

  ZmqChannel publisher() {
    ZmqChannel.Builder builder = ZmqChannel.PUB(ctx);
    Props props = Props.builder().withBindAddr(inproc("PubSubTest")).build();
    return with(builder.with(props).build());
  }

  ZmqChannel subscriber() {
    ZmqChannel.Builder builder = ZmqChannel.SUB(ctx);
    Props props = Props.builder().withConnectAddr(inproc("PubSubTest")).build();
    return with(builder.with(props).build());
  }
}
