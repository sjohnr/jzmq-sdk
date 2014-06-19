package org.zeromq.messaging;

import org.zeromq.support.ObjectBuilder;

import static com.google.common.base.Preconditions.checkArgument;

public final class ZmqMessage {

  public static final byte[] EMPTY_FRAME = "".getBytes();
  public static final byte BYTE_SUB = 1; // denotes subscribe request.
  public static final byte BYTE_UNSUB = 0; // denotes unsubscribe request.

  public static final class Builder implements ObjectBuilder<ZmqMessage> {

    private ZmqMessage _target = new ZmqMessage();

    private Builder() {
    }

    private Builder(ZmqMessage message) {
      _target.topic = message.topic;
      _target.identities = new ZmqFrames(message.identities);
      _target.headers = message.headers;
      _target.payload = message.payload;
    }

    @Override
    public ZmqMessage build() {
      return _target;
    }

    public Builder withTopic(byte[] topic) {
      checkArgument(topic != null);
      _target.topic = topic;
      return this;
    }

    public Builder withIdentities(ZmqFrames frames) {
      _target.identities = new ZmqFrames(frames.size());
      for (byte[] identity : frames) {
        _target.identities.add(identity);
      }
      return this;
    }

    public Builder withHeaders(byte[] headers) {
      checkArgument(headers != null);
      _target.headers = headers;
      return this;
    }

    public Builder withPayload(byte[] payload) {
      checkArgument(payload != null);
      _target.payload = payload;
      return this;
    }

    public Builder withExtendedPubSubFlag(byte extendedPubSubFlag) {
      checkArgument(extendedPubSubFlag == BYTE_SUB || extendedPubSubFlag == BYTE_UNSUB);
      _target.extendedPubSubFlag = extendedPubSubFlag;
      return this;
    }
  }

  /**
   * PUB/SUB/XPUB/XSUB topic.
   * <p/>
   * <b>NOTE: this field is optional.</b>
   */
  private byte[] topic = EMPTY_FRAME;
  /**
   * ZMQ' socket peer_identity container.
   * <p/>
   * IN-coming/OUT-going processing flow for socket identities looks as following:
   * <pre>
   *   Network                    Application              Network
   *
   *   [peer_identity_0]                               [peer_identity_0]
   *   []                      [peer_identity_0]       []
   *   [peer_identity_i]  ==>  [peer_identity_i]  ==>  [peer_identity_i]
   *   []                      [peer_identity_n]       []
   *   [peer_identity_n]                               [peer_identity_n]
   *   []
   * </pre>
   * <p/>
   * <b>NOTE: this field is optional.</b>
   */
  private ZmqFrames identities = new ZmqFrames();
  /**
   * Headers container. Headers contain info which is needed for framework to function.
   * <p/>
   * <b>NOTE: this field is optional.</b>
   */
  private byte[] headers = EMPTY_FRAME;
  /**
   * The BLOB or aka PAYLOAD. Contains end user application data.
   * <p/>
   * <b>NOTE: this field is optional.</b>
   */
  private byte[] payload = EMPTY_FRAME;
  /**
   * Byte denoting whether this is SUBSCRIBE message or UNSUBSCRIBE one. It's only make
   * sense in the context of XPUB or XSUB.
   * <p/>
   * <b>NOTE: this field is optional.</b>
   */
  private byte extendedPubSubFlag = -1;

  //// CONSTRUCTORS

  private ZmqMessage() {
  }

  //// METHODS

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(ZmqMessage message) {
    return new Builder(message);
  }

  public byte[] topic() {
    return topic;
  }

  public ZmqFrames identityFrames() {
    return new ZmqFrames(identities);
  }

  public byte[] headers() {
    return headers;
  }

  public byte[] payload() {
    return payload;
  }

  public boolean isSubscribe() {
    return extendedPubSubFlag == BYTE_SUB;
  }

  public boolean isUnsubscribe() {
    return extendedPubSubFlag == BYTE_UNSUB;
  }

  public byte extendedPubSubFlag() {
    return extendedPubSubFlag;
  }
}
