package org.zeromq.messaging.chat;

import org.zeromq.messaging.BaseFixture;
import org.zeromq.messaging.Props;
import org.zeromq.messaging.ZmqContext;
import org.zeromq.support.thread.ZmqProcess;

class ChatFixture extends BaseFixture {

  ChatFixture(ZmqContext ctx) {
    super(ctx);
  }

  void chat(String frontendPub,
            String clusterPub,
            String frontendSub,
            String clusterPubConnAddr) {
    with(
        ZmqProcess.builder()
                  .with(Chat.builder()
                            .with(ctx)
                            .withPollTimeout(100)
                            .withFrontendPub(Props.builder().withBindAddr(frontendPub).build())
                            .withClusterPub(Props.builder().withBindAddr(clusterPub).build())
                            .withFrontendSub(Props.builder().withBindAddr(frontendSub).build())
                            .withClusterSub(Props.builder().withConnectAddr(clusterPubConnAddr).build())
                            .build()
                  )
                  .build()
    );
  }
}
