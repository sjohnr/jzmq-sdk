package org.zeromq.messaging.service;

import org.zeromq.messaging.ZmqFrames;

interface ZmqRouting {

	void putRouting(ZmqFrames identities);

	ZmqFrames getRouting(ZmqFrames identities);

	int available();
}
