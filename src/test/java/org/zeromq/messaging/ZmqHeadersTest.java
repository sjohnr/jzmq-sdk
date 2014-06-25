package org.zeromq.messaging;

import org.junit.Test;

import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.zeromq.support.ZmqUtils.EMPTY_FRAME;

public class ZmqHeadersTest {

  @Test
  public void t0() {
    ZmqHeaders headers = ZmqHeaders.builder().build(); // empty headers.

    assertNull(headers.getHeader(""));
    assertNull(headers.getHeader("".getBytes()));
    assertNull(headers.getHeader("h0".getBytes()));
    assertEquals("", new String(headers.asBinary()));
  }

  @Test
  public void t1() {
    ZmqHeaders headers = ZmqHeaders.builder("ctx_id=abc,h111=xyz789,cnx_id=11rr,h2=,h3=,h737=v737".getBytes()).build();

    assertEquals("11rr", headers.getHeader("cnx_id".getBytes())); // expecting header.
    assertEquals("abc", headers.getHeader("ctx_id".getBytes())); // expecting header.
    assertEquals("xyz789", headers.getHeader("h111".getBytes())); // expecting header.
    assertEquals("", headers.getHeader("h2".getBytes())); // expecting header.
    assertEquals("", headers.getHeader("h3".getBytes())); // expecting header.
    assertEquals("v737", headers.getHeader("h737".getBytes())); // expecting header.
    assertNull(headers.getHeader("h4".getBytes())); // not expecting header.
    assertNull(headers.getHeader("h0")); // not expecting header by header name.
  }

  @Test
  public void t2() {
    ZmqHeaders.builder(EMPTY_FRAME);
    ZmqHeaders.builder(new byte[0]);

    ZmqHeaders headers = ZmqHeaders.builder("0=,1=,2=".getBytes()).build();
    assertEquals("", headers.getHeader("0".getBytes())); // expecting header.
    assertEquals("", headers.getHeader("1".getBytes())); // expecting header.
    assertEquals("", headers.getHeader("2".getBytes())); // expecting header.
  }

  @Test
  public void t3() {
    ZmqHeaders headers = ZmqHeaders.builder().set("a", "a").set("b", "b").build();
    assertEquals("a=a,b=b", new String(headers.asBinary()));
    assertEquals("a", headers.getHeader("a"));
    assertEquals("b", headers.getHeader("b"));

    ZmqHeaders reset = ZmqHeaders.builder().set("a", "x").set("b", "").set("1", "1").build();
    assertEquals("x", reset.getHeader("a"));
    assertEquals("", reset.getHeader("b"));
    assertEquals("1", reset.getHeader("1"));
    assertEquals("a=x,b=,1=1", new String(reset.asBinary()));
  }

  @Test
  public void t4() {
    ZmqHeaders headers = ZmqHeaders.builder().set("a", "a").set("b", "b").build();
    assertEquals("a=a,b=b", new String(headers.asBinary()));

    ZmqHeaders merge = ZmqHeaders.builder(headers.asBinary()).set("c", "c").set("d", "d").build();
    assertEquals("a=a,b=b,c=c,d=d", new String(merge.asBinary()));
    assertEquals("c", merge.getHeader("c"));
    assertEquals("d", merge.getHeader("d"));
    assertEquals("a", merge.getHeader("a".getBytes()));
    assertEquals("b", merge.getHeader("b".getBytes()));
  }

  @Test
  public void t5() {
    ZmqHeaders headers = ZmqHeaders.builder("h0=v0,h1=v1".getBytes()).build();
    assertEquals("h0=v0,h1=v1", new String(headers.asBinary()));
  }
}
