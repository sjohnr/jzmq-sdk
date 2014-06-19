package org.zeromq.support.thread;

import org.zeromq.support.HasDestroy;
import org.zeromq.support.HasInit;

public interface ZmqActor extends HasInit, HasDestroy {

  void exec() throws Exception;
}
