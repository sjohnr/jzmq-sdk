package org.zeromq.messaging.device.service;

import org.zeromq.messaging.ZmqMessage;

/**
 * Entry point for processing incoming {@link ZmqMessage} on behalf of {@link ZmqAbstractWorker}.
 * <p/>
 * Implementations should encapsulte parsing of incoming {@link ZmqMessage},
 * deserializing his payload and passing object down in the call chain.
 * Reverse process is similar: object should be converted back to the
 * {@link ZmqMessage} message.
 * <p/>
 * <b>NOTE: returning back reply message is optional.</b>
 */
public interface ZmqMessageProcessor {

  /**
   * {@link org.zeromq.messaging.ZmqMessage} processor function.
   *
   * @param message incoming request_message.
   * @return reply_message or {@code null} if reply is not required.
   */
  ZmqMessage process(ZmqMessage message) throws Exception;
}
