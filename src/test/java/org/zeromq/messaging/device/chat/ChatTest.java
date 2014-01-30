package org.zeromq.messaging.device.chat;

import org.junit.Test;
import org.zeromq.messaging.Props;
import org.zeromq.messaging.ZmqAbstractTest;
import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqMessage;

public class ChatTest extends ZmqAbstractTest {

  @Test
  public void t0() throws InterruptedException {
    ChatFixture f = new ChatFixture(ctx());

    f.chat(inprocAddr("p>>"),
           inprocAddr("p>>>>"),
           inprocAddr("s<<"),
           inprocAddr("p>>>>"));

    f.init();

    try {
      ZmqChannel pub = ZmqChannel.PUB(ctx())
                                 .withProps(Props.builder()
                                                 .withConnAddress(inprocAddr("p>>"))
                                                 .build())
                                 .build();

      ZmqChannel sub = ZmqChannel.SUB(ctx())
                                 .withProps(Props.builder()
                                                 .withConnAddress(inprocAddr("s<<"))
                                                 .build())
                                 .build();

      byte[] topic = "xxx".getBytes();
      sub.subscribe(topic);

      waitSec(); // wait a second.

      pub.send(ZmqMessage.builder(HELLO()).withTopic(topic).build());
      assertPayload("hello", sub.recv());
    }
    finally {
      f.destroy();
    }
  }
}
