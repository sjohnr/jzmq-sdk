package org.zeromq.messaging.device.chat;

import org.junit.Test;
import org.zeromq.messaging.Props;
import org.zeromq.messaging.ZmqAbstractTest;
import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqMessage;

import java.util.concurrent.TimeUnit;

public class ChatTest extends ZmqAbstractTest {

  @Test
  public void t0() throws InterruptedException {
    ChatFixture f = new ChatFixture(ctx());

    int frontendPub = 3030;
    int frontendSub = 4040;
    int clusterPub = 3035;

    f.chat(bindAddr(frontendPub), // -->>
           bindAddr(clusterPub),  // -->>>
           bindAddr(frontendSub), // <<--
           connAddr(clusterPub)); // <<<<--
    f.init();

    try {
      ZmqChannel pub = ZmqChannel.builder()
                                 .withCtx(ctx())
                                 .PUB()
                                 .withProps(Props.builder()
                                                 .withConnAddress(connAddr(frontendPub))
                                                 .build())
                                 .build();

      ZmqChannel sub = ZmqChannel.builder()
                                 .withCtx(ctx())
                                 .SUB()
                                 .withProps(Props.builder()
                                                 .withConnAddress(connAddr(frontendSub))
                                                 .build())
                                 .build();

      TimeUnit.SECONDS.sleep(1);

      byte[] topic = "xxx".getBytes();
      ZmqMessage message = ZmqMessage.builder(HELLO()).withTopic(topic).build();
      sub.subscribe(topic);
      pub.send(message);

      TimeUnit.SECONDS.sleep(1);
    }
    finally {
      f.destroy();
    }
  }
}
