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

import static org.zeromq.messaging.ZmqMessage.DIV_FRAME;
import static org.zeromq.messaging.ZmqMessage.EMPTY_FRAME;

/**
 * Output message adapter. Handles transformation of outgoing {@link ZmqMessage} into {@link ZmqFrames}.
 * <p/>
 * Transformation schemes:
 * <pre>
 * ZmqMessage([[HEADER] | PAYLOAD])                           => ZmqFrames([FRAME, ..., FRAME]).
 * ZmqMessage([[PEER_IDENTITY] | [HEADER] | PAYLOAD])         => ZmqFrames([FRAME, ..., FRAME]).
 * ZmqMessage([TOPIC | [HEADER] | PAYLOAD])                   => ZmqFrames([FRAME, ..., FRAME]).
 * ZmqMessage([TOPIC | [PEER_IDENTITY] | [HEADER] | PAYLOAD]) => ZmqFrames([FRAME, ..., FRAME]).
 * </pre>
 */
class OutputMessageAdapter implements ObjectAdapter<ZmqMessage, ZmqFrames> {

  private static final Logger LOG = LoggerFactory.getLogger(OutputMessageAdapter.class);

  public static class Builder implements ObjectBuilder<OutputMessageAdapter> {

    private final OutputMessageAdapter _target = new OutputMessageAdapter();

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

    @Override
    public void checkInvariant() {
      if (_target.awareOfTopicFrame && _target.awareOfDEALERType) {
        throw new IllegalStateException();
      }
      if (_target.awareOfDEALERType && !_target.expectIdentities) {
        throw new IllegalStateException();
      }
    }

    @Override
    public OutputMessageAdapter build() {
      checkInvariant();
      return _target;
    }
  }

  private boolean awareOfTopicFrame;
  private boolean awareOfDEALERType;
  private boolean expectIdentities;

  //// CONSTRUCTORS

  private OutputMessageAdapter() {
  }

  //// METHODS

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public ZmqFrames convert(ZmqMessage message) throws ZmqException {
    try {
      ZmqFrames target = new ZmqFrames();

      // --- topic

      if (awareOfTopicFrame) {
        target.add(message.topic());
        target.add(DIV_FRAME);
      }

      // --- identities

      if (expectIdentities) {
        // don't forget special DEALER case ...
        if (awareOfDEALERType) {
          target.add(EMPTY_FRAME);
        }
        for (byte[] frame : message.identities()) {
          target.add(frame);
          target.add(EMPTY_FRAME);
        }
        target.add(DIV_FRAME);
      }

      // --- headers

      target.addAll(message.headers());
      target.add(DIV_FRAME);

      // --- payload

      target.add(message.payload());

      return target;
    }
    catch (Exception e) {
      LOG.error("!!! Failed to convert outgoing ZmqMessage: " + e, e);
      throw ZmqException.wrap(e);
    }
  }
}
