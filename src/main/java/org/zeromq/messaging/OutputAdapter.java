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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.support.HasInvariant;
import org.zeromq.support.ObjectAdapter;
import org.zeromq.support.ObjectBuilder;
import org.zeromq.support.ZmqUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.zeromq.messaging.ZmqMessage.EMPTY_FRAME;

/**
 * Output message adapter. Handles transformation of outgoing {@link ZmqMessage} into {@link ZmqFrames}.
 * <p/>
 * Transformation schemes:
 * <pre>
 * ZmqMessage([[HEADER] | PAYLOAD])                           => ZmqFrames([FRAME, ..., FRAME]).
 * ZmqMessage([[PEER_IDENTITY] | [HEADER] | PAYLOAD])         => ZmqFrames([FRAME, ..., FRAME]).
 * ZmqMessage([TOPIC | [HEADER] | PAYLOAD])                   => ZmqFrames([FRAME, ..., FRAME]).
 * </pre>
 */
class OutputAdapter implements ObjectAdapter<ZmqMessage, ZmqFrames> {

  private static final Logger LOG = LoggerFactory.getLogger(OutputAdapter.class);

  public static class Builder implements ObjectBuilder<OutputAdapter>, HasInvariant {

    private final OutputAdapter _target = new OutputAdapter();

    private Builder() {
    }

    public Builder awareOfTopicFrame() {
      _target.awareOfTopicFrame = true;
      return this;
    }

    public Builder awareOfDEALERType() {
      _target.awareOfDEALERType = true;
      return this;
    }

    public Builder expectIdentities() {
      _target.expectIdentities = true;
      return this;
    }

    public Builder awareOfExtendedPubSub() {
      _target.awareOfExtendedPubSub = true;
      return this;
    }

    @Override
    public void checkInvariant() {
      // no-op.
    }

    @Override
    public OutputAdapter build() {
      return _target;
    }
  }

  private boolean awareOfTopicFrame;
  private boolean awareOfDEALERType;
  private boolean expectIdentities;
  private boolean awareOfExtendedPubSub;

  //// CONSTRUCTORS

  private OutputAdapter() {
  }

  //// METHODS

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public ZmqFrames convert(ZmqMessage message) {
    try {
      ZmqFrames target = new ZmqFrames();

      // -- if this is XPUB or XSUB then handle message accordingly.
      if (awareOfExtendedPubSub) {
        byte[] flag = {message.extendedPubSubFlag()};
        byte[] topic = message.topic();
        target.add(ZmqUtils.mergeBytes(Arrays.asList(flag, topic)));
        return target;
      }

      // --- topic

      if (awareOfTopicFrame) {
        target.add(message.topic());
      }

      // --- identities

      if (expectIdentities) {
        // don't forget special DEALER case ...
        if (awareOfDEALERType) {
          target.add(EMPTY_FRAME);
        }
        for (byte[] frame : message.identityFrames()) {
          target.add(frame);
          target.add(EMPTY_FRAME);
        }
        target.add(EMPTY_FRAME);
      }

      // --- headers_size, headers, payload_size, payload

      byte[] headers = message.headersAsBinary();
      byte[] payload = message.payload();
      ByteBuffer buf = ByteBuffer.allocate(4/* sizeOf headers.len */ + headers.length + 4/* sizeOf payload.len */ + payload.length);
      buf.putInt(headers.length).put(headers).putInt(payload.length).put(payload);
      target.add(buf.array());

      return target;
    }
    catch (Exception e) {
      LOG.error("!!! Failed to convert outgoing ZmqMessage: " + e, e);
      throw ZmqException.seeCause(e);
    }
  }
}
