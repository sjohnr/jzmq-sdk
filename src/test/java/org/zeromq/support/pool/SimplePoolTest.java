package org.zeromq.support.pool;

import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import org.junit.Test;
import org.zeromq.Checker;
import org.zeromq.support.HasDestroy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertSame;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SimplePoolTest {

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

  final PoolObjectLifecycle<String> testStrBuilder = new PoolObjectLifecycle<String>() {

    private int c = 0;

    @Override
    public String build() {
      return new String("cool" + (++c));
    }

    @Override
    public void destroy(String string) {
      // no-op for test.
    }
  };

  @Test
  public void t0() {
    SimplePool<String> pool = new SimplePool<String>(testStrBuilder);

    assertThat(pool.capacity(), is(SimplePool.DEFAULT_CAPACITY));
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
    SimplePool<String> pool = new SimplePool<String>(1, testStrBuilder);

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
  public void t2() throws InterruptedException {
    SimplePool<String> pool = new SimplePool<String>(1, testStrBuilder);

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
    SimplePool<String> pool = new SimplePool<String>(Short.MAX_VALUE, testStrBuilder);

    Checker checker = new Checker();
    CountDownLatch l4 = new CountDownLatch(4);

    int ITER_NUM = 100000;
    leaseReleaseFast(l4, ITER_NUM, pool, checker);
    leaseReleaseFast(l4, ITER_NUM, pool, checker);
    leaseReleaseFast(l4, ITER_NUM, pool, checker);
    leaseReleaseFast(l4, ITER_NUM, pool, checker);

    l4.await();

    assert checker.passed();
    assert pool.available() <= 4;
  }

  @Test
  public void t4() throws InterruptedException {
    SimplePool<String> pool = new SimplePool<String>(Short.MAX_VALUE, testStrBuilder);

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
                                  final SimplePool<String> pool,
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
                                  final SimplePool<String> pool,
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
