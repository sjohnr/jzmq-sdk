/*
 * Copyright (c) 2012 artem.vysochyn@gmail.com
 * Copyright (c) 2013 Other contributors as noted in the AUTHORS file
 *
 * jzmq-sdk is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation; either version 3 of the License, or
 * (at your option) any later version.
 *
 * jzmq-sdk is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * jzmq-sdk became possible because of jzmq binding and zmq library itself.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.zeromq.messaging;

import org.zeromq.support.ObjectBuilder;

import static com.google.common.base.Preconditions.checkArgument;
import static org.zeromq.support.ZmqUtils.isEmptyFrame;

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
      _target.headers = new ZmqHeaders().copy(message.headers);
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
        checkArgument(!isEmptyFrame(identity));
        _target.identities.add(identity);
      }
      return this;
    }

    public Builder withHeaders(ZmqHeaders headers) {
      _target.headers = new ZmqHeaders().copy(headers);
      return this;
    }

    public Builder withHeaders(byte[] headers) {
      _target.headers = new ZmqHeaders().copy(headers);
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
   * SPI headers container. Headers contain info which is needed for framework to function.
   * Different devices may put their auxiliary info in headers. With that said, headers should be
   * treated as something which allow certain type of devices to function.
   * <p/>
   * By default, headers initialized to empty structure.
   * Use {@link Builder#withHeaders(byte[])} or {@link Builder#withHeaders(ZmqHeaders)}.
   * <p/>
   * <b>
   * NOTE: end user applications should not access headers!
   * Clients may want to  access/modify/create_their_own  headers when they need to
   * construct new devices, or <i>calling patterns</i>.
   * This field is optional.
   * </b>
   */
  private ZmqHeaders headers = new ZmqHeaders();
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

  public byte[] headersAsBinary() {
    return headers.asBinary();
  }

  /**
   * Function returns a <i>view</i> of headers specified by {@code viewClass}.
   *
   * @param viewClass the headers-view-class. <b>Have to have no-arg constructor.</b>
   */
  @SuppressWarnings("unchecked")
  public <T extends ZmqHeaders> T headersAs(Class<T> viewClass) {
    try {
      return (T) viewClass.newInstance().copy(headers);
    }
    catch (Exception e) {
      throw ZmqException.seeCause(e);
    }
  }

  public ZmqHeaders headers() {
    return headersAs(ZmqHeaders.class);
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
