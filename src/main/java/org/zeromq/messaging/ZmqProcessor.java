package org.zeromq.messaging;

public interface ZmqProcessor {

  ZmqFrames process(ZmqFrames frames) throws Exception;
}
