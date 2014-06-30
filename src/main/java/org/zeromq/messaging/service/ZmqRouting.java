package org.zeromq.messaging.service;

import org.zeromq.messaging.ZmqFrames;

public interface ZmqRouting {

	void putRouting(ZmqFrames route);

	ZmqFrames getRouting(ZmqFrames route);

	int available();
}
