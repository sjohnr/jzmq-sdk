package org.zeromq.messaging;

import com.google.common.collect.ImmutableList;
import org.zeromq.support.HasInvariant;
import org.zeromq.support.ObjectBuilder;

import java.nio.ByteBuffer;

import static org.zeromq.messaging.ZmqMessage.EMPTY_FRAME;
import static org.zeromq.support.ZmqUtils.mergeBytes;

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
final class OutputAdapter {

  final static class Builder implements ObjectBuilder<OutputAdapter>, HasInvariant {

    private final OutputAdapter _target = new OutputAdapter();

    private Builder() {
    }

    Builder awareOfTopicFrame() {
      _target.awareOfTopicFrame = true;
      return this;
    }

    Builder awareOfDEALERType() {
      _target.awareOfDEALERType = true;
      return this;
    }

    Builder expectIdentities() {
      _target.expectIdentities = true;
      return this;
    }

    Builder awareOfExtendedPubSub() {
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

  private final byte[] _inprocRefBuf = new byte[4/*integer*/];

  //// CONSTRUCTORS

  private OutputAdapter() {
  }

  //// METHODS

  static Builder builder() {
    return new Builder();
  }

  ZmqFrames convert(ZmqMessage message) {
    ZmqFrames target = new ZmqFrames();

    // -- if this is XPUB or XSUB then handle message accordingly.
    if (awareOfExtendedPubSub) {
      byte[] flag = {message.extendedPubSubFlag()};
      byte[] topic = message.topic();
      target.add(mergeBytes(ImmutableList.of(flag, topic)));
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

    byte[] headers = message.headers();
    byte[] payload = message.payload();
    ByteBuffer buf = ByteBuffer.allocate(4/* sizeOf headers.len */ +
                                         headers.length +
                                         4/* sizeOf payload.len */ +
                                         payload.length);
    buf.putInt(headers.length).put(headers).putInt(payload.length).put(payload);
    target.add(buf.array());

    return target;
  }

  ZmqFrames convertInprocRef(ZmqMessage message) {
    ZmqFrames target = new ZmqFrames();

    // -- if this is XPUB or XSUB then handle message accordingly --
    if (awareOfExtendedPubSub) {
      byte[] flag = {message.extendedPubSubFlag()};
      byte[] topic = message.topic();
      target.add(mergeBytes(ImmutableList.of(flag, topic)));
      return target;
    }

    // --- topic --
    if (awareOfTopicFrame) {
      target.add(message.topic());
    }

    // --- identities --
    if (expectIdentities) {
      if (awareOfDEALERType) {
        target.add(EMPTY_FRAME);
      }
      for (byte[] frame : message.identityFrames()) {
        target.add(frame);
        target.add(EMPTY_FRAME);
      }
      target.add(EMPTY_FRAME);
    }

    // --- inprocRef --
    int i = message.inprocRef();
    _inprocRefBuf[0] = (byte) (i >> 24);
    _inprocRefBuf[1] = (byte) (i >> 16);
    _inprocRefBuf[2] = (byte) (i >> 8);
    _inprocRefBuf[3] = (byte) i;
    target.add(_inprocRefBuf);

    return target;
  }
}
