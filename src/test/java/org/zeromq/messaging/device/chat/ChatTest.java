package org.zeromq.messaging.device.chat;

import org.junit.Test;
import org.zeromq.messaging.Props;
import org.zeromq.messaging.ZmqAbstractTest;
import org.zeromq.messaging.ZmqChannel;

import java.util.concurrent.TimeUnit;

public class ChatTest extends ZmqAbstractTest {

  @Test
  public void t0() throws InterruptedException {
    ChatFixture f = new ChatFixture(ctx());

    String talking_gala = "gala>>>";
    String listening_gala = "gala<<<";
    String talking_alenka = "alenka>>>";
    String listening_alenka = "alenka<<<";

    f.chat(inprocAddr(talking_gala), bindAddr(4040), inprocAddr(listening_gala), connAddr(5050));
    f.chat(inprocAddr(talking_alenka), bindAddr(5050), inprocAddr(listening_alenka), connAddr(4040));

    f.init();
    try {
      ZmqChannel gala_says = ZmqChannel.builder()
                                       .withCtx(ctx())
                                       .ofPUBType()
                                       .withProps(Props.builder()
                                                       .withConnectAddress(inprocAddr(talking_gala))
                                                       .build())
                                       .build();

      ZmqChannel gala_listens = ZmqChannel.builder()
                                          .withCtx(ctx())
                                          .ofSUBType()
                                          .withProps(Props.builder()
                                                          .withConnectAddress(inprocAddr(listening_gala))
                                                          .build())
                                          .build();

      ZmqChannel alenka_says = ZmqChannel.builder()
                                         .withCtx(ctx())
                                         .ofPUBType()
                                         .withProps(Props.builder()
                                                         .withConnectAddress(inprocAddr(talking_alenka))
                                                         .build())
                                         .build();

      ZmqChannel alenka_listens = ZmqChannel.builder()
                                            .withCtx(ctx())
                                            .ofSUBType()
                                            .withProps(Props.builder()
                                                            .withConnectAddress(inprocAddr(listening_alenka))
                                                            .build())
                                            .build();

      TimeUnit.SECONDS.sleep(1);

      byte[] topic = "xxx".getBytes();

      gala_listens.subscribe(topic);
      alenka_listens.subscribe(topic);

      TimeUnit.SECONDS.sleep(1);
    }
    finally {
      f.destroy();
    }
  }
}
