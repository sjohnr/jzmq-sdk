package org.zeromq.messaging;

import org.junit.After;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

import static org.zeromq.messaging.ZmqFrames.EMPTY_FRAME;

public abstract class ZmqAbstractTest {

  private final ZmqContext _ctx = new ZmqContext();

  protected final ZmqContext c() {
    return _ctx;
  }

  @Before
  public final void init() {
    _ctx.init();
  }

  @After
  public final void cleanup() {
    _ctx.destroy();
  }

  public static ZmqFrames emptyIdentities() {
    return new ZmqFrames();
  }

  public static byte[] emptyPayload() {
    return EMPTY_FRAME;
  }

  public static byte[] payload() {
    return "payload".getBytes();
  }

  public static String conn(int port) {
    return "tcp://localhost:" + port;
  }

  public static String bind(int port) {
    return "tcp://*:" + port;
  }

  public static String inproc(String addr) {
    return "inproc://" + addr;
  }

  public static void waitSec() throws InterruptedException {
    TimeUnit.SECONDS.sleep(1);
  }
}
