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

package org.zeromq.support.pool;

import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import org.junit.Test;
import org.zeromq.Checker;
import org.zeromq.support.HasDestroy;
import org.zeromq.support.ObjectBuilder;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertSame;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SimpleObjectPoolTest {

  static class String implements HasDestroy {

    CharSequence str;

    String(CharSequence str) {
      this.str = str;
    }

    @Override
    public void destroy() {
      str = null;
    }
  }

  final ObjectBuilder<String> testStrBuilder = new ObjectBuilder<String>() {

    private int c = 0;

    @Override
    public String build() {
      return new String("cool" + (++c));
    }
  };

  @Test
  public void t0() {
    SimpleObjectPool<String> pool = new SimpleObjectPool<String>(testStrBuilder);

    assertThat(pool.capacity(), is(SimpleObjectPool.DEFAULT_CAPACITY));
    assertThat(pool.available(), is(0));

    Lease<String> s = pool.lease();
    assertThat(pool.available(), is(0));
    assertEquals("cool1", s.get().str);

    s.release();
    assertThat(pool.available(), is(1));

    Lease<String> x = pool.lease();
    assertThat(pool.available(), is(0));
    assertSame(s, x);

    x.release();
    assertThat(pool.available(), is(1));
  }

  @Test
  public void t1() {
    SimpleObjectPool<String> pool = new SimpleObjectPool<String>(1, testStrBuilder);

    Lease<String> s = pool.lease();
    assertThat(pool.available(), is(0));

    // check that it's permitted to release same
    // object several times with out negative consequences.

    s.release();
    s.release();
    s.release();
    s.release();

    assertThat(pool.available(), is(1));
    assertSame(s, pool.lease());
  }

  @Test
  public void t2() {
    SimpleObjectPool<String> pool = new SimpleObjectPool<String>(1, testStrBuilder);

    Lease<String> lease = pool.lease();
    assertEquals("cool1", lease.get().str);

    assert pool.lease() == null;

    Stopwatch timer = new Stopwatch().start();
    int timeout = 100;
    assert pool.lease(timeout) == null;
    assert timer.stop().elapsedMillis() >= timeout;

    lease.release();
    assertEquals("cool1", pool.lease().get().str);
  }

  @Test
  public void t3() throws InterruptedException {
    SimpleObjectPool<String> pool = new SimpleObjectPool<String>(Short.MAX_VALUE, testStrBuilder);

    Checker checker = new Checker();
    CountDownLatch l4 = new CountDownLatch(4);

    int ITER_NUM = 100000;
    leaseReleaseFast(l4, ITER_NUM, pool, checker);
    leaseReleaseFast(l4, ITER_NUM, pool, checker);
    leaseReleaseFast(l4, ITER_NUM, pool, checker);
    leaseReleaseFast(l4, ITER_NUM, pool, checker);

    l4.await();

    assert checker.passed();
    assertEquals(4, pool.available());
  }

  @Test
  public void t4() throws InterruptedException {
    SimpleObjectPool<String> pool = new SimpleObjectPool<String>(Short.MAX_VALUE, testStrBuilder);

    Checker checker = new Checker();
    CountDownLatch l8 = new CountDownLatch(8);

    int ITER_NUM = 64;
    leaseReleaseSlow(l8, ITER_NUM, pool, checker);
    leaseReleaseFast(l8, ITER_NUM, pool, checker);
    leaseReleaseSlow(l8, ITER_NUM, pool, checker);
    leaseReleaseFast(l8, ITER_NUM, pool, checker);
    leaseReleaseSlow(l8, ITER_NUM, pool, checker);
    leaseReleaseFast(l8, ITER_NUM, pool, checker);
    leaseReleaseSlow(l8, ITER_NUM, pool, checker);
    leaseReleaseFast(l8, ITER_NUM, pool, checker);

    l8.await();

    assert checker.passed();
    assert pool.available() <= 8;
  }

  private Thread leaseReleaseFast(final CountDownLatch l,
                                  final int iterNum,
                                  final SimpleObjectPool<String> pool,
                                  final Checker checker) {
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          for (int i = 0; i < iterNum; i++) {
            pool.lease().release();
          }
        }
        finally {
          l.countDown();
        }
      }
    });
    t.setUncaughtExceptionHandler(checker);
    t.setDaemon(true);
    t.start();
    return t;
  }

  private Thread leaseReleaseSlow(final CountDownLatch l,
                                  final int iterNum,
                                  final SimpleObjectPool<String> pool,
                                  final Checker checker) {
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          for (int i = 0; i < iterNum; i++) {
            Lease<String> o = pool.lease();
            TimeUnit.MILLISECONDS.sleep(16);
            o.release();
          }
        }
        catch (Throwable e) {
          Throwables.propagate(e);
        }
        finally {
          l.countDown();
        }
      }
    });
    t.setUncaughtExceptionHandler(checker);
    t.setDaemon(true);
    t.start();
    return t;
  }
}
