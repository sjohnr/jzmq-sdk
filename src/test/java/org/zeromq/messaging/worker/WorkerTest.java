package org.zeromq.messaging.worker;

import org.junit.Test;
import org.zeromq.messaging.ZmqAbstractTest;

public class WorkerTest extends ZmqAbstractTest {

  @Test
  public void t0() {
    WorkerFixture f = new WorkerFixture(ctx());

    // ...

    f.init();
    try {

    }
    finally {
      f.destroy();
    }
  }
}
