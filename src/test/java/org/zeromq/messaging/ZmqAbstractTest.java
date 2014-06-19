package org.zeromq.messaging;

import org.junit.After;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;

public abstract class ZmqAbstractTest {

  private final ZmqContext _ctx = new ZmqContext();

  protected final ZmqContext ctx() {
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

  public static ZmqMessage HELLO() {
    return ZmqMessage.builder().withHeaders(HEADERS()).withPayload("hello".getBytes()).build();
  }

  public static ZmqMessage WORLD() {
    return ZmqMessage.builder().withHeaders(HEADERS()).withPayload("world".getBytes()).build();
  }

  public static ZmqMessage CARP() {
    return ZmqMessage.builder().withHeaders(HEADERS()).withPayload("carp".getBytes()).build();
  }

  public static ZmqMessage SHIRT() {
    return ZmqMessage.builder().withHeaders(HEADERS()).withPayload("shirt".getBytes()).build();
  }

  public static byte[] HEADERS() {
    return ZmqHeaders.builder().set("zmq", "is").set("so", "much").set("co", "col").build().asBinary();
  }

  public static void assertPayload(String payload, ZmqMessage message) {
    assert message != null;
    assert message.payload() != null;
    assertEquals(payload, new String(message.payload()));
  }

  public static String connAddr(int port) {
    return "tcp://localhost:" + port;
  }

  public static String bindAddr(int port) {
    return "tcp://*:" + port;
  }

  public static String notAvailConnAddr0() {
    return "tcp://localhost:667";
  }

  public static String notAvailConnAddr1() {
    return "tcp://localhost:670";
  }

  public static String inprocAddr(String addr) {
    return "inproc://" + addr;
  }

  public static void waitSec() throws InterruptedException {
    TimeUnit.SECONDS.sleep(1);
  }
}
