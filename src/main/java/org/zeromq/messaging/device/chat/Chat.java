package org.zeromq.messaging.device.chat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.messaging.Props;
import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqException;
import org.zeromq.messaging.ZmqFrames;
import org.zeromq.messaging.device.ZmqAbstractActor;

import static org.zeromq.ZMQ.DONTWAIT;
import static org.zeromq.support.ZmqUtils.BYTE_SUB;
import static org.zeromq.support.ZmqUtils.BYTE_UNSUB;
import static org.zeromq.support.ZmqUtils.matchExtPubSub;
import static org.zeromq.support.ZmqUtils.matchExtPubSubTopic;
import static org.zeromq.support.ZmqUtils.matchPayload;
import static org.zeromq.support.ZmqUtils.matchTopic;

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

    register(CHANNEL_ID_FRONTEND_PUB, ZmqChannel.XSUB(ctx).withProps(frontendPubProps).build());
    register(CHANNEL_ID_CLUSTER_PUB, ZmqChannel.XPUB(ctx).withProps(clusterPubProps).build());
    register(CHANNEL_ID_FRONTEND_SUB, ZmqChannel.XPUB(ctx).withProps(frontendSubProps).build());
    register(CHANNEL_ID_CLUSTER_SUB, ZmqChannel.XSUB(ctx).withProps(clusterSubProps).build());

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
        ZmqFrames frames = frontendPub.recv(DONTWAIT);
        if (frames == null) {
          break;
        }
        clusterPub.sendFrames(frames, DONTWAIT);
        logMessage("local --> cluster", frames);
      }
    }
    if (clusterPub.canRecv()) {
      for (; ; ) {
        ZmqFrames frames = clusterPub.recv(DONTWAIT);
        if (frames == null) {
          break;
        }
        frontendPub.sendFrames(frames, DONTWAIT);
        byte b = matchExtPubSub(frames);
        byte[] topic = matchExtPubSubTopic(frames);
        if (b == BYTE_SUB) {
          logSubscribe("local <-- cluster", topic);
        }
        else if (b == BYTE_UNSUB) {
          logUnsubscribe("local <-- cluster", topic);
        }
      }
    }
    if (clusterSub.canRecv()) {
      for (; ; ) {
        ZmqFrames frames = clusterSub.recv(DONTWAIT);
        if (frames == null) {
          break;
        }
        frontendSub.sendFrames(frames, DONTWAIT);
        logMessage("local <-- cluster", frames);
      }
    }
    if (frontendSub.canRecv()) {
      for (; ; ) {
        ZmqFrames frames = frontendSub.recv(DONTWAIT);
        if (frames == null) {
          break;
        }
        clusterSub.sendFrames(frames, DONTWAIT);
        byte b = matchExtPubSub(frames);
        byte[] topic = matchExtPubSubTopic(frames);
        if (b == BYTE_SUB) {
          logSubscribe("local --> cluster", topic);
        }
        else if (b == BYTE_UNSUB) {
          logUnsubscribe("local --> cluster", topic);
        }
      }
    }
  }

  private void logMessage(String direction, ZmqFrames frames) {
    if (LOG.isDebugEnabled()) {
      byte[] topic = matchTopic(frames);
      byte[] payload = matchPayload(frames);
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
