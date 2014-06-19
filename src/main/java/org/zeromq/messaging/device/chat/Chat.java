package org.zeromq.messaging.device.chat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.messaging.Props;
import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqException;
import org.zeromq.messaging.ZmqMessage;
import org.zeromq.messaging.device.ZmqAbstractActor;

public final class Chat extends ZmqAbstractActor {

  private static final Logger LOG = LoggerFactory.getLogger(Chat.class);

  /**
   * XSUB -- for serving connecting publishers:
   * <pre>
   *       byte[] <--      <--conn-- PUB
   *                  XSUB <--conn-- PUB
   *    sub/unsub -->      <--conn-- PUB
   * </pre>
   */
  private static final String CHANNEL_ID_FRONTEND_PUB = "frontendPub";
  /**
   * XPUB -- for serving cluster wide subscribers:
   * <pre>
   *      byte[] -->      <--conn-- cluster XSUB
   *                 XPUB <--conn-- cluster XSUB
   *   sub/unsub <--      <--conn-- cluster XSUB
   * </pre>
   */
  private static final String CHANNEL_ID_CLUSTER_PUB = "clusterPub";
  /**
   * XPUB -- for serving connecting subscribers:
   * <pre>
   *    sub/unsub <--      <--conn-- SUB
   *                  XPUB <--conn-- SUB
   *       byte[] -->      <--conn-- SUB
   * </pre>
   */
  private static final String CHANNEL_ID_FRONTEND_SUB = "frontendSub";
  /**
   * XSUB -- for serving cluster wide publishers:
   * <pre>
   *      byte[] <--      --conn--> cluster XPUB
   *                 XSUB --conn--> cluster XPUB
   *   sub/unsub -->      --conn--> cluster XPUB
   * </pre>
   */
  private static final String CHANNEL_ID_CLUSTER_SUB = "clusterSub";

  public static final class Builder extends ZmqAbstractActor.Builder<Builder, Chat> {

    public Builder() {
      super(new Chat());
    }

    public Builder withFrontendPubProps(Props frontendPubProps) {
      _target.setFrontendPubProps(frontendPubProps);
      return this;
    }

    public Builder withClusterPubProps(Props clusterPubProps) {
      _target.setClusterPubProps(clusterPubProps);
      return this;
    }

    public Builder withFrontendSubProps(Props frontendSubProps) {
      _target.setFrontendSubProps(frontendSubProps);
      return this;
    }

    public Builder withClusterSubProps(Props clusterSubProps) {
      _target.setClusterSubProps(clusterSubProps);
      return this;
    }
  }

  private Props frontendPubProps;
  private Props clusterPubProps;
  private Props frontendSubProps;
  private Props clusterSubProps;

  //// METHDOS

  public static Builder builder() {
    return new Builder();
  }

  public void setFrontendPubProps(Props frontendPubProps) {
    this.frontendPubProps = frontendPubProps;
  }

  public void setClusterPubProps(Props clusterPubProps) {
    this.clusterPubProps = clusterPubProps;
  }

  public void setFrontendSubProps(Props frontendSubProps) {
    this.frontendSubProps = frontendSubProps;
  }

  public void setClusterSubProps(Props clusterSubProps) {
    this.clusterSubProps = clusterSubProps;
  }

  @Override
  public void checkInvariant() {
    super.checkInvariant();
    if (frontendPubProps.bindAddr().isEmpty()) {
      throw ZmqException.fatal();
    }
    if (clusterPubProps.bindAddr().isEmpty()) {
      throw ZmqException.fatal();
    }
    if (frontendSubProps.bindAddr().isEmpty()) {
      throw ZmqException.fatal();
    }
    if (clusterSubProps.connectAddr().isEmpty()) {
      throw ZmqException.fatal();
    }
  }

  @Override
  public void init() {
    checkInvariant();

    reg(CHANNEL_ID_FRONTEND_PUB, ZmqChannel.XSUB(ctx).withProps(frontendPubProps).build());
    reg(CHANNEL_ID_CLUSTER_PUB, ZmqChannel.XPUB(ctx).withProps(clusterPubProps).build());
    reg(CHANNEL_ID_FRONTEND_SUB, ZmqChannel.XPUB(ctx).withProps(frontendSubProps).build());
    reg(CHANNEL_ID_CLUSTER_SUB, ZmqChannel.XSUB(ctx).withProps(clusterSubProps).build());

    // By default, unconditionally, Chat is set to handle duplicate subscriptions/unsubscriptions.
    channel(CHANNEL_ID_CLUSTER_PUB).setExtendedPubSubVerbose();
    channel(CHANNEL_ID_FRONTEND_SUB).setExtendedPubSubVerbose();

    channel(CHANNEL_ID_FRONTEND_PUB).watchRecv(_poller);
    channel(CHANNEL_ID_CLUSTER_PUB).watchRecv(_poller);
    channel(CHANNEL_ID_FRONTEND_SUB).watchRecv(_poller);
    channel(CHANNEL_ID_CLUSTER_SUB).watchRecv(_poller);
  }

  @Override
  public void exec() {
    super.exec();

    ZmqChannel frontendPub = channel(CHANNEL_ID_FRONTEND_PUB);
    ZmqChannel clusterPub = channel(CHANNEL_ID_CLUSTER_PUB);
    ZmqChannel clusterSub = channel(CHANNEL_ID_CLUSTER_SUB);
    ZmqChannel frontendSub = channel(CHANNEL_ID_FRONTEND_SUB);

    if (frontendPub.canRecv()) {
      for (; ; ) {
        ZmqMessage message = frontendPub.recvDontWait();
        if (message == null) {
          break;
        }
        clusterPub.send(message);
        logMessage("local --> cluster", message);
      }
    }
    if (clusterPub.canRecv()) {
      for (; ; ) {
        ZmqMessage message = clusterPub.recvDontWait();
        if (message == null) {
          break;
        }
        frontendPub.send(message);
        if (message.isSubscribe()) {
          logSubscribe("local <-- cluster", message.topic());
        }
        else if (message.isUnsubscribe()) {
          logUnsubscribe("local <-- cluster", message.topic());
        }
      }
    }
    if (clusterSub.canRecv()) {
      for (; ; ) {
        ZmqMessage message = clusterSub.recvDontWait();
        if (message == null) {
          break;
        }
        frontendSub.send(message);
        logMessage("local <-- cluster", message);
      }
    }
    if (frontendSub.canRecv()) {
      for (; ; ) {
        ZmqMessage message = frontendSub.recvDontWait();
        if (message == null) {
          break;
        }
        clusterSub.send(message);
        if (message.isSubscribe()) {
          logSubscribe("local --> cluster", message.topic());
        }
        else if (message.isUnsubscribe()) {
          logUnsubscribe("local --> cluster", message.topic());
        }
      }
    }
  }

  private void logMessage(String direction, ZmqMessage message) {
    if (LOG.isDebugEnabled()) {
      byte[] topic = message.topic();
      byte[] payload = message.payload();
      LOG.debug("Message: {} (topic={} bytes, payload={} bytes).", direction, topic.length, payload.length);
    }
  }

  private void logSubscribe(String direction, byte[] topic) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Subscribe: {} (topic={} bytes).", direction, topic.length);
    }
  }

  private void logUnsubscribe(String direction, byte[] topic) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Unsubscribe: {} (topic={} bytes).", direction, topic.length);
    }
  }
}
