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
import org.zeromq.support.ObjectAdapter;
import org.zeromq.support.ObjectBuilder;

import static org.zeromq.support.ZmqUtils.isDivFrame;
import static org.zeromq.support.ZmqUtils.isEmptyFrame;

/**
 * Input message adapter. Handles transformation of incoming {@link ZmqFrames} into {@link ZmqMessage}.
 * <p/>
 * Transformation schemes:
 * <pre>
 * ZmqFrames([FRAME, ..., FRAME]) => ZmqMessage([[HEADER] | PAYLOAD]).
 * ZmqFrames([FRAME, ..., FRAME]) => ZmqMessage([[PEER_IDENTITY] | [HEADER] | PAYLOAD]).
 * ZmqFrames([FRAME, ..., FRAME]) => ZmqMessage([TOPIC | [HEADER] | PAYLOAD]).
 * ZmqFrames([FRAME, ..., FRAME]) => ZmqMessage([TOPIC | [PEER_IDENTITY] | [HEADER] | PAYLOAD]).
 * </pre>
 */
class InputMessageAdapter implements ObjectAdapter<ZmqFrames, ZmqMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(InputMessageAdapter.class);

  public static class Builder implements ObjectBuilder<InputMessageAdapter> {

    private final InputMessageAdapter _target = new InputMessageAdapter();

    private Builder() {
    }

    public Builder awareOfTopicFrame() {
      _target.awareOfTopicFrame = true;
      return this;
    }

    public Builder expectIdentities() {
      _target.expectIdentities = true;
      return this;
    }

    @Override
    public void checkInvariant() {
      // no-op.
    }

    @Override
    public InputMessageAdapter build() {
      checkInvariant();
      return _target;
    }
  }

  private boolean awareOfTopicFrame;
  private boolean expectIdentities;

  //// CONSTRUCTORS

  private InputMessageAdapter() {
  }

  //// METHODS

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public ZmqMessage convert(ZmqFrames frames) {
    try {
      ZmqMessage.Builder builder = ZmqMessage.builder();

      // --- topic

      if (awareOfTopicFrame) {
        builder.withTopic(frames.poll());
        // consume [-] frame.
        if (!isDivFrame(frames.poll())) {
          throw ZmqException.wrongHeader();
        }
      }

      // --- identities

      if (expectIdentities) {
        ZmqFrames identities = new ZmqFrames();
        for (; ; ) {
          byte[] frame = frames.poll();
          if (isDivFrame(frame)) {
            // is current frame [-] then stop.
            break;
          }
          if (isEmptyFrame(frame)) {
            // is [ ] current frame then skip.
            continue;
          }
          identities.add(frame);
        }
        builder.withIdentities(identities);
      }

      // --- headers

      builder.withHeaders(frames.poll());

      // --- payload

      builder.withPayload(frames.poll());

      return builder.build();
    }
    catch (Exception e) {
      LOG.error("!!! Failed to convert incoming ZmqFrames: " + e, e);
      throw ZmqException.seeCause(e);
    }
  }
}
