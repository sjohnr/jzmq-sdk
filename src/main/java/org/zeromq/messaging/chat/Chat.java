package org.zeromq.messaging.chat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.messaging.Props;
import org.zeromq.messaging.ZmqAbstractActor;
import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqFrames;

import static com.google.common.base.Preconditions.checkArgument;
import static org.zeromq.ZMQ.DONTWAIT;
import static org.zeromq.messaging.ZmqFrames.BYTE_SUB;
import static org.zeromq.messaging.ZmqFrames.BYTE_UNSUB;

public final class Chat extends ZmqAbstractActor {

  private static final Logger LOGGER = LoggerFactory.getLogger(Chat.class);

  /**
   * XSUB -- for serving connecting publishers:
   * <pre>
   *       byte[] <--      <--conn-- PUB
   *                  XSUB <--conn-- PUB
   *    sub/unsub -->      <--conn-- PUB
   * </pre>
   */
  private static final String FRONTEND_PUB = "frontendPub";
  /**
   * XPUB -- for serving cluster wide subscribers:
   * <pre>
   *      byte[] -->      <--conn-- cluster XSUB
   *                 XPUB <--conn-- cluster XSUB
   *   sub/unsub <--      <--conn-- cluster XSUB
   * </pre>
   */
  private static final String CLUSTER_PUB = "clusterPub";
  /**
   * XPUB -- for serving connecting subscribers:
   * <pre>
   *    sub/unsub <--      <--conn-- SUB
   *                  XPUB <--conn-- SUB
   *       byte[] -->      <--conn-- SUB
   * </pre>
   */
  private static final String FRONTEND_SUB = "frontendSub";
  /**
   * XSUB -- for serving cluster wide publishers:
   * <pre>
   *      byte[] <--      --conn--> cluster XPUB
   *                 XSUB --conn--> cluster XPUB
   *   sub/unsub -->      --conn--> cluster XPUB
   * </pre>
   */
  private static final String CLUSTER_SUB = "clusterSub";

  public static final class Builder extends ZmqAbstractActor.Builder<Builder, Chat> {

    public Builder() {
      super(new Chat());
    }

    public Builder withFrontendPub(Props props) {
      _target.setFrontendPub(props);
      return this;
    }

    public Builder withClusterPub(Props clusterPub) {
      _target.setClusterPub(clusterPub);
      return this;
    }

    public Builder withFrontendSub(Props props) {
      _target.setFrontendSub(props);
      return this;
    }

    public Builder withClusterSub(Props props) {
      _target.setClusterSub(props);
      return this;
    }
  }

  private Props frontendPub;
  private Props clusterPub;
  private Props frontendSub;
  private Props clusterSub;

  //// CONSTRUCTORS

  private Chat() {
  }

  //// METHDOS

  public static Builder builder() {
    return new Builder();
  }

  public void setFrontendPub(Props frontendPub) {
    this.frontendPub = frontendPub;
  }

  public void setClusterPub(Props clusterPub) {
    this.clusterPub = clusterPub;
  }

  public void setFrontendSub(Props frontendSub) {
    this.frontendSub = frontendSub;
  }

  public void setClusterSub(Props clusterSub) {
    this.clusterSub = clusterSub;
  }

  @Override
  public void checkInvariant() {
    super.checkInvariant();
    checkArgument(!frontendPub.bindAddr().isEmpty());
    checkArgument(!clusterPub.bindAddr().isEmpty());
    checkArgument(!frontendSub.bindAddr().isEmpty());
    checkArgument(!clusterSub.connectAddr().isEmpty());
  }

  @Override
  public void init() {
    checkInvariant();

    put(FRONTEND_PUB, ZmqChannel.XSUB(ctx).with(frontendPub).build()).watchRecv(_poller);
    put(CLUSTER_PUB, ZmqChannel.XPUB(ctx).with(clusterPub).build()).watchRecv(_poller);
    put(FRONTEND_SUB, ZmqChannel.XPUB(ctx).with(frontendSub).build()).watchRecv(_poller);
    put(CLUSTER_SUB, ZmqChannel.XSUB(ctx).with(clusterSub).build()).watchRecv(_poller);

    // By default, unconditionally, Chat is set to handle duplicate subscriptions/unsubscriptions.
    get(CLUSTER_PUB).setExtendedPubSubVerbose();
    get(FRONTEND_SUB).setExtendedPubSubVerbose();
  }

  @Override
  public void exec() throws Exception {
    poll();

    ZmqChannel frontendPub = get(FRONTEND_PUB);
    ZmqChannel clusterPub = get(CLUSTER_PUB);
    ZmqChannel clusterSub = get(CLUSTER_SUB);
    ZmqChannel frontendSub = get(FRONTEND_SUB);

    if (frontendPub.canRecv()) {
      for (; ; ) {
        ZmqFrames frames = frontendPub.recv(DONTWAIT);
        if (frames == null)
          break;

        clusterPub.sendFrames(frames, DONTWAIT);
        logMessage("local --> cluster", frames);
      }
    }

    if (clusterPub.canRecv()) {
      for (; ; ) {
        ZmqFrames frames = clusterPub.recv(DONTWAIT);
        if (frames == null)
          break;

        frontendPub.sendFrames(frames, DONTWAIT);
        byte b = frames.getExtPubSub();
        byte[] topic = frames.getExtPubSubTopic();
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
        if (frames == null)
          break;

        frontendSub.sendFrames(frames, DONTWAIT);
        logMessage("local <-- cluster", frames);
      }
    }

    if (frontendSub.canRecv()) {
      for (; ; ) {
        ZmqFrames frames = frontendSub.recv(DONTWAIT);
        if (frames == null)
          break;

        clusterSub.sendFrames(frames, DONTWAIT);
        byte b = frames.getExtPubSub();
        byte[] topic = frames.getExtPubSubTopic();
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
    if (LOGGER.isDebugEnabled()) {
      byte[] topic = frames.getTopic();
      byte[] payload = frames.getPayload();
      LOGGER.debug("Message: {} (topic={} bytes, payload={} bytes).", direction, topic.length, payload.length);
    }
  }

  private void logSubscribe(String direction, byte[] topic) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Subscribe: {} (topic={} bytes).", direction, topic.length);
    }
  }

  private void logUnsubscribe(String direction, byte[] topic) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Unsubscribe: {} (topic={} bytes).", direction, topic.length);
    }
  }
}
