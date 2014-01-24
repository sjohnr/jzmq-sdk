package org.zeromq.messaging.device.chat;

import org.zeromq.messaging.BaseFixture;
import org.zeromq.messaging.Props;
import org.zeromq.messaging.ZmqContext;
import org.zeromq.support.thread.ZmqRunnable;

public class ChatFixture extends BaseFixture {

  private final ZmqContext zmqContext;

  public ChatFixture(ZmqContext zmqContext) {
    this.zmqContext = zmqContext;
  }

  void chat(String localPubAddr,
            String clusterPubAddr,
            String localSubAddr,
            String clusterSubAddr) {
    with(
        ZmqRunnable.builder()
                   .withRunnableContext(
                       Chat.builder()
                           .withZmqContext(zmqContext)
                           .withLocalPublisherProps(
                               Props.builder()
                                    .withBindAddress(localPubAddr)
                                    .build())
                           .withClusterPublisherProps(
                               Props.builder()
                                    .withBindAddress(clusterPubAddr)
                                    .build())
                           .withLocalSubscriberProps(
                               Props.builder()
                                    .withBindAddress(localSubAddr)
                                    .build())
                           .withClusterSubscriberProps(
                               Props.builder()
                                    .withConnectAddress(clusterSubAddr)
                                    .build())
                           .build()
                   )
                   .build()
    );
  }
}
