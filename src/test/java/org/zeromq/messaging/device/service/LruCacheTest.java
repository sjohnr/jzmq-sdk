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

package org.zeromq.messaging.device.service;

import com.google.common.base.Stopwatch;
import org.junit.Test;
import org.zeromq.TestRecorder;
import org.zeromq.messaging.ZmqException;
import org.zeromq.messaging.ZmqFrames;

import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class LruCacheTest {

  private static final int TIMEOUT = 100;
  private static int OPS = 100000;
  private static int ITER = 10;

  @Test
  public void t0() throws InterruptedException {
    TestRecorder r = new TestRecorder().start();
    r.log("Store identity, expire identity, catch exception on accessing empty cache.");

    LruCache target = new LruCache(TIMEOUT);

    ZmqFrames backendIdentities = new ZmqFrames();
    backendIdentities.add("b_0".getBytes());
    backendIdentities.add("b_1".getBytes());
    backendIdentities.add("b_2".getBytes());

    target.store(backendIdentities);

    assertEquals(1, target.size());

    TimeUnit.MILLISECONDS.sleep(TIMEOUT + 100);

    ZmqFrames frontendIdentities = new ZmqFrames();
    frontendIdentities.add("f_0".getBytes());
    frontendIdentities.add("f_1".getBytes());
    frontendIdentities.add("f_2".getBytes());

    try {
      target.obtain(frontendIdentities);
      fail();
    }
    catch (ZmqException e) {
      assertEquals(ZmqException.ErrorCode.SOCKET_IDENTITY_STORAGE_IS_EMPTY, e.errorCode());
    }
  }

  @Test
  public void t1() {
    TestRecorder r = new TestRecorder().start();
    r.log("Obtain identity from empty cache, catch exception on accessing empty cache.");

    LruCache target = new LruCache(TIMEOUT);

    assertEquals(0, target.size());

    ZmqFrames frontendIdentities = new ZmqFrames();
    frontendIdentities.add("f_0".getBytes());
    frontendIdentities.add("f_1".getBytes());
    frontendIdentities.add("f_2".getBytes());

    try {
      target.obtain(frontendIdentities);
      fail();
    }
    catch (ZmqException e) {
      assertEquals(ZmqException.ErrorCode.SOCKET_IDENTITY_STORAGE_IS_EMPTY, e.errorCode());
    }
  }

  @Test
  public void t2() {
    TestRecorder r = new TestRecorder().start();
    r.log("Store identity, obtain identity, after that check that cache is empty.");

    LruCache target = new LruCache(TIMEOUT);

    ZmqFrames backendIdentities = new ZmqFrames();
    backendIdentities.add("b_0".getBytes());
    backendIdentities.add("b_1".getBytes());
    backendIdentities.add("b_2".getBytes());

    target.store(backendIdentities);

    assertEquals(1, target.size());

    ZmqFrames frontendIdentities = new ZmqFrames();
    frontendIdentities.add("f_0".getBytes());
    frontendIdentities.add("f_1".getBytes());
    frontendIdentities.add("f_2".getBytes());

    assertSame(backendIdentities, target.obtain(frontendIdentities));

    assertEquals(0, target.size());
  }

  @Test
  public void t3() {
    TestRecorder r = new TestRecorder().start();
    r.log("Store identity, obtain identity, check that given identity comparator is being called.");

    LruCache target =
        new LruCache(TIMEOUT,
                     new Comparator<byte[]>() {
                       @Override
                       public int compare(byte[] frontend, byte[] backend) {
                         if (Arrays.equals("f".getBytes(), frontend) &&
                             Arrays.equals("b".getBytes(), backend)) {
                           return 0;
                         }
                         return -1;
                       }
                     });

    ZmqFrames backendIdentities = new ZmqFrames();
    backendIdentities.add("x".getBytes());
    backendIdentities.add("y".getBytes());
    backendIdentities.add("z".getBytes());
    backendIdentities.add("b".getBytes());

    target.store(backendIdentities);

    assertEquals(1, target.size());

    ZmqFrames frontendIdentities = new ZmqFrames();
    frontendIdentities.add("xx".getBytes());
    frontendIdentities.add("yy".getBytes());
    frontendIdentities.add("zz".getBytes());
    frontendIdentities.add("f".getBytes());

    assertSame(backendIdentities, target.obtain(frontendIdentities));

    assertEquals(0, target.size());
  }

  @Test
  public void t4_perf() {
    TestRecorder r = new TestRecorder();
    r.log("Storage is empty. Hit rate is high.");

    LruCache target = new LruCache(100);
    Stopwatch timer = new Stopwatch().start();
    for (int j = 0; j < ITER; j++) {
      for (int i = 0; i < OPS; i++) {
        try {
          target.obtain(new ZmqFrames());
        }
        catch (ZmqException ignore) {
        }
      }
    }
    r.logQoS(timer.stop().elapsedTime(MICROSECONDS) / (ITER * OPS), "microsec/.obtain().");
  }

  @Test
  public void t5_perf() {
    TestRecorder r = new TestRecorder();
    r.log("Storage is not empty, but no matched socket identity will be found. Hit rate is high.");

    LruCache target =
        new LruCache(Long.MAX_VALUE,
                     new Comparator<byte[]>() {
                       @Override
                       public int compare(byte[] frontend, byte[] backend) {
                         return -1;
                       }
                     });
    ZmqFrames x = new ZmqFrames();
    x.add("x".getBytes());
    for (int i = 0; i < 1000; i++) {
      target.store(x);
    }

    Stopwatch timer = new Stopwatch().start();
    for (int j = 0; j < ITER; j++) {
      for (int i = 0; i < OPS; i++) {
        try {
          target.obtain(x);
        }
        catch (ZmqException ignore) {
        }
      }
    }
    r.logQoS(timer.stop().elapsedTime(MICROSECONDS) / (ITER * OPS), "microsec/.obtain().");
  }
}
