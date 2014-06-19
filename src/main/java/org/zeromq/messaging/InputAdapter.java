package org.zeromq.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.support.HasInvariant;
import org.zeromq.support.ObjectAdapter;
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
class InputAdapter implements ObjectAdapter<ZmqFrames, ZmqMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(InputAdapter.class);

  public static class Builder implements ObjectBuilder<InputAdapter>, HasInvariant {

    private final InputAdapter _target = new InputAdapter();

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

    public Builder awareOfExtendedPubSub() {
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

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public ZmqMessage convert(ZmqFrames frames) {
    try {
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
    catch (Exception e) {
      LOG.error("!!! Failed to convert incoming ZmqFrames: " + e, e);
      throw ZmqException.seeCause(e);
    }
  }
}
