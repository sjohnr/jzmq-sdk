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
import static org.zeromq.support.ZmqUtils.isDivFrame;
import static org.zeromq.support.ZmqUtils.isEmptyFrame;

public final class ZmqMessage {

  public static final byte[] EMPTY_FRAME = "".getBytes();
  public static final byte[] DIV_FRAME = new byte[]{'\u001D'}; // group separator.

  public static final class Builder implements ObjectBuilder<ZmqMessage> {

    private ZmqMessage _target = new ZmqMessage();

    private Builder() {
    }

    private Builder(ZmqMessage message) {
      withTopic(message.topic());
      withIdentities(message.identities());
      withHeaders(message.headers());
      withPayload(message.payload());
    }

    @Override
    public void checkInvariant() {
      // no-op.
    }

    @Override
    public ZmqMessage build() {
      checkInvariant();
      return _target;
    }

    /** See {@link ZmqMessage#topic}. */
    public Builder withTopic(byte[] topic) {
      checkArgument(!isDivFrame(topic));
      _target.topic = topic;
      return this;
    }

    /** See {@link ZmqMessage#identities}. */
    public Builder withIdentity(byte[] identity) {
      checkArgument(!isEmptyFrame(identity));
      checkArgument(!isDivFrame(identity));
      _target.identities.add(identity);
      return this;
    }

    /** See {@link ZmqMessage#identities}. */
    public Builder withIdentities(ZmqFrames frames) {
      for (byte[] identity : frames) {
        checkArgument(!isEmptyFrame(identity));
        checkArgument(!isDivFrame(identity));
        _target.identities.add(identity);
      }
      return this;
    }

    /**
     * See {@link ZmqMessage#headers}.
     * Expected source -- externally constructed headers.
     */
    public Builder withHeaders(ZmqHeaders headers) {
      _target.headers = new ZmqHeaders().copy(headers);
      return this;
    }

    /**
     * See {@link ZmqMessage#headers}.
     * Expected source -- headers (which will be parsed in corresponding way).
     */
    public Builder withHeaders(byte[] headers) {
      _target.headers.copy(headers);
      return this;
    }

    /** See {@link ZmqMessage#payload}. */
    public Builder withPayload(byte[] payload) {
      checkArgument(!isDivFrame(payload));
      _target.payload = payload;
      return this;
    }
  }

  /**
   * PUB/SUB feature.
   * <p/>
   * <b>NOTE: this field is optional. Used for PUB/SUB.</b>
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

  public ZmqFrames identities() {
    return identities;
  }

  public byte[] headers() {
    return headers.asBinary();
  }

  /**
   * Function returns a <i>projection</i> of headers specified by {@code wrapperClass}.
   *
   * @param wrapperClass wrapper class or <i>headers projection</i> class.
   */
  @SuppressWarnings("unchecked")
  public <T extends ZmqHeaders> T headersAs(Class<T> wrapperClass) {
    try {
      return (T) wrapperClass.newInstance().copy(headers);
    }
    catch (Exception e) {
      throw ZmqException.seeCause(e);
    }
  }

  public byte[] payload() {
    return payload;
  }
}
