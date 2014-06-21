package org.zeromq.messaging;

import org.zeromq.support.HasInvariant;
import org.zeromq.support.ObjectBuilder;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.zeromq.messaging.ZmqMessage.EMPTY_FRAME;
import static org.zeromq.support.ZmqUtils.isEmptyFrame;

/**
 * Input message adapter. Handles transformation of incoming {@link ZmqFrames} into {@link ZmqMessage}.
 * <p/>
 * Transformation schemes:
 * <pre>
 * ZmqFrames([FRAME, ..., FRAME]) => ZmqMessage([[HEADER] | PAYLOAD]).
 * ZmqFrames([FRAME, ..., FRAME]) => ZmqMessage([[PEER_IDENTITY] | [HEADER] | PAYLOAD]).
 * ZmqFrames([FRAME, ..., FRAME]) => ZmqMessage([TOPIC | [HEADER] | PAYLOAD]).
 * </pre>
 */
final class InputAdapter {

  final static class Builder implements ObjectBuilder<InputAdapter>, HasInvariant {

    private final InputAdapter _target = new InputAdapter();

    private Builder() {
    }

    Builder awareOfTopicFrame() {
      _target.awareOfTopicFrame = true;
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
    public InputAdapter build() {
      return _target;
    }
  }

  private boolean awareOfTopicFrame;
  private boolean expectIdentities;
  private boolean awareOfExtendedPubSub;

  //// CONSTRUCTORS

  private InputAdapter() {
  }

  //// METHODS

  static Builder builder() {
    return new Builder();
  }

  ZmqMessage convert(ZmqFrames frames) {
    if (frames == null || frames.isEmpty()) {
      return null;
    }

    ZmqMessage.Builder builder = ZmqMessage.builder();

    // -- if this is XPUB or XSUB then handle frames accordingly.
    if (awareOfExtendedPubSub) {
      byte[] buf = frames.poll();
      builder.withExtendedPubSubFlag(buf[0]);
      builder.withTopic(buf.length > 1 ? Arrays.copyOfRange(buf, 1, buf.length) : EMPTY_FRAME);
      return builder.build();
    }

    // --- topic

    if (awareOfTopicFrame) {
      builder.withTopic(frames.poll());
    }

    // --- identities

    if (expectIdentities) {
      ZmqFrames identities = new ZmqFrames();
      for (int emptyFrameSeen = 0; ; ) {
        byte[] frame = frames.poll();
        if (isEmptyFrame(frame)) {
          ++emptyFrameSeen;
        }
        if (emptyFrameSeen == 2) {
          break;
        }
        if (!isEmptyFrame(frame)) {
          emptyFrameSeen = 0;
          identities.add(frame);
        }
      }
      builder.withIdentities(identities);
    }

    // --- headers_size, headers, payload_size, payload

    ByteBuffer buf = ByteBuffer.wrap(frames.poll());

    byte[] headers = new byte[buf.getInt()];
    buf.get(headers);
    builder.withHeaders(headers);

    byte[] payload = new byte[buf.getInt()];
    buf.get(payload);
    builder.withPayload(payload);

    return builder.build();
  }

  ZmqMessage convertInprocRef(ZmqFrames frames) {
    if (frames == null || frames.isEmpty()) {
      return null;
    }

    ZmqMessage.Builder builder = ZmqMessage.builder();

    // -- if this is XPUB or XSUB then handle frames accordingly --
    if (awareOfExtendedPubSub) {
      byte[] buf = frames.poll();
      builder.withExtendedPubSubFlag(buf[0]);
      builder.withTopic(buf.length > 1 ? Arrays.copyOfRange(buf, 1, buf.length) : EMPTY_FRAME);
      return builder.build();
    }

    // --- topic --
    if (awareOfTopicFrame) {
      builder.withTopic(frames.poll());
    }

    // --- identities --
    if (expectIdentities) {
      ZmqFrames identities = new ZmqFrames();
      for (int emptyFrameSeen = 0; ; ) {
        byte[] frame = frames.poll();
        if (isEmptyFrame(frame)) {
          ++emptyFrameSeen;
        }
        if (emptyFrameSeen == 2) {
          break;
        }
        if (!isEmptyFrame(frame)) {
          emptyFrameSeen = 0;
          identities.add(frame);
        }
      }
      builder.withIdentities(identities);
    }

    // --- inprocRef --
    builder.withInprocRef(frames.poll());

    return builder.build();
  }
}
