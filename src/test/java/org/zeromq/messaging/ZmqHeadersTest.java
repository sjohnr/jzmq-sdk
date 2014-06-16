/*
 * Copyright (c) 2012 artem.vysochyn@gmail.com
 * Copyright (c) 2013 Other contributors as noted in the AUTHORS file
 *
 * jzmq-sdk is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation; either version 3 of the License, or
 * (at your option) any later version.
 *
 * jzmq-sdk is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * jzmq-sdk became possible because of jzmq binding and zmq library itself.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.zeromq.messaging;

import org.junit.Test;

import static java.util.regex.Pattern.compile;
import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.zeromq.messaging.ZmqMessage.EMPTY_FRAME;

public class ZmqHeadersTest {

  @Test
  public void t0() {
    ZmqHeaders headers = ZmqHeaders.builder().build(); // empty headers.

    assertNull(headers.getHeader(""));
    assertEquals("", new String(headers.asBinary()));
  }

  @Test
  public void t1() {
    ZmqHeaders headers = ZmqHeaders.builder("h0=abc,h1=xyz789,h2=,h3=".getBytes()).build();

    assertEquals("abc", headers.getHeader(compile("h0=(\\w*)[,]?"))); // expecting header by regexp.
    assertEquals("xyz789", headers.getHeader(compile("h1=(\\w*)[,]?"))); // expecting header by regexp.
    assertEquals("", headers.getHeader(compile("h2=(\\w*)[,]?"))); // expecting header by regexp.
    assertEquals("", headers.getHeader(compile("h3=(\\w*)[,]?"))); // expecting header by regexp.
    assertNull(headers.getHeader(compile("h4=(\\w*)[,]?"))); // not expecting header even by regexp.
    assertNull(headers.getHeader("h0")); // not expecting header by header name.
  }

  @Test
  public void t2() {
    ZmqHeaders.builder(EMPTY_FRAME);
    ZmqHeaders.builder(new byte[0]);

    ZmqHeaders headers = ZmqHeaders.builder("0=,1=,2=".getBytes()).build();
    assertEquals("", headers.getHeader(compile("0=(\\w*)[,]?"))); // expecting header by regexp.
    assertEquals("", headers.getHeader(compile("1=(\\w*)[,]?"))); // expecting header by regexp.
    assertEquals("", headers.getHeader(compile("2=(\\w*)[,]?"))); // expecting header by regexp.
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
    assertEquals("a", merge.getHeader(compile("a=(\\w*)[,]?")));
    assertEquals("b", merge.getHeader(compile("b=(\\w*)[,]?")));
    assertEquals("c", merge.getHeader("c"));
    assertEquals("d", merge.getHeader("d"));
  }
}
