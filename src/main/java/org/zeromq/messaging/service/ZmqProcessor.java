package org.zeromq.messaging.service;

import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqFrames;

public interface ZmqProcessor {

  void accept(ZmqFrames route, byte[] payload, ZmqChannel outcome, ZmqRouting outcomeRouting) throws Exception;

  void comeback(ZmqFrames route, byte[] payload, ZmqChannel outcome, ZmqRouting outcomeRouting) throws Exception;
}
