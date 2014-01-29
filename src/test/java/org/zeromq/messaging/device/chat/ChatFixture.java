package org.zeromq.messaging.device.chat;

import org.zeromq.messaging.BaseFixture;
import org.zeromq.messaging.Props;
import org.zeromq.messaging.ZmqContext;
import org.zeromq.support.thread.ZmqRunnable;

public class ChatFixture extends BaseFixture {

  private final ZmqContext ctx;

  public ChatFixture(ZmqContext ctx) {
    this.ctx = ctx;
  }

  void chat(String frontendPub,
            String clusterPub,
            String frontendSub,
            String clusterSub) {
    with(
        ZmqRunnable.builder()
                   .withRunnableContext(
                       Chat.builder()
                           .withCtx(ctx)
                           .withFrontendPubProps(
                               Props.builder()
                                    .withBindAddress(frontendPub)
                                    .build())
                           .withClusterPubProps(
                               Props.builder()
                                    .withBindAddress(clusterPub)
                                    .build())
                           .withFrontendSubProps(
                               Props.builder()
                                    .withBindAddress(frontendSub)
                                    .build())
                           .withClusterSubProps(
                               Props.builder()
                                    .withConnectAddress(clusterSub)
                                    .build())
                           .build()
                   )
                   .build()
    );
  }
}
