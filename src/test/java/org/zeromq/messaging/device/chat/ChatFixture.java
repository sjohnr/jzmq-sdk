package org.zeromq.messaging.device.chat;

import org.zeromq.messaging.BaseFixture;
import org.zeromq.messaging.Props;
import org.zeromq.messaging.ZmqContext;
import org.zeromq.messaging.chat.Chat;
import org.zeromq.support.thread.ZmqProcess;

public class ChatFixture extends BaseFixture {

  private final ZmqContext ctx;

  public ChatFixture(ZmqContext ctx) {
    this.ctx = ctx;
  }

  void chat(String frontendPub,
            String clusterPub,
            String frontendSub,
            String clusterPubConnAddr) {
    with(
        ZmqProcess.builder()
                  .withActor(
                      Chat.builder()
                          .withCtx(ctx)
                          .withPollTimeout(100)
                          .withFrontendPubProps(Props.builder().withBindAddr(frontendPub).build())
                          .withClusterPubProps(Props.builder().withBindAddr(clusterPub).build())
                          .withFrontendSubProps(Props.builder().withBindAddr(frontendSub).build())
                          .withClusterSubProps(Props.builder().withConnectAddr(clusterPubConnAddr).build())
                          .build()
                  )
                  .build()
    );
  }
}
