package org.zeromq.messaging.device.chat;

import org.zeromq.messaging.BaseFixture;
import org.zeromq.messaging.Props;
import org.zeromq.support.thread.ZmqRunnable;

public class ChatFixture extends BaseFixture {

  void chat(String localPubAddr, String clusterPubAddr, String localSubAddr, String clusterSubAddr) {
    with(
        ZmqRunnable.builder()
                   .withRunnableContext(
                       Chat.builder()
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
                                    .withBindAddress(clusterSubAddr)
                                    .build())
                           .build()
                   )
                   .build()
    );
  }
}
