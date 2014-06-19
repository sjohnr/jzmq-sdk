package org.zeromq.support.pool;

import java.util.BitSet;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public final class SimplePool<T> implements Pool<T> {

  public static final int DEFAULT_CAPACITY = 64; // just best guess.

  private static class LeaseImpl<T> implements Lease<T> {

    final SimplePool<T> pool;
    final int ind;
    final T obj;

    LeaseImpl(SimplePool<T> pool, int ind, T obj) {
      this.pool = pool;
      this.ind = ind;
      this.obj = obj;
    }

    @Override
    public T get() {
      return obj;
    }

    @Override
    public void release() {
      pool._state.set(ind, true); // set to 1 - free.
      pool.indexes.offerFirst(ind);
    }

    void destroy() {
      pool.lifecycle.destroy(obj);
    }
  }

  private final int capacity;
  private final PoolObjectLifecycle<T> lifecycle;

  private final LeaseImpl[] _pool;
  private final BlockingDeque<Integer> indexes;
  private final BitSet _state; // 1 - obj in pool is free, 0 - obj in pool is busy.

  //// CONSTRUCTORS

  public SimplePool(PoolObjectLifecycle<T> lifecycle) {
    this(DEFAULT_CAPACITY, lifecycle);
  }

  public SimplePool(int capacity, PoolObjectLifecycle<T> lifecycle) {
    this.capacity = capacity;
    this.lifecycle = lifecycle;

    _pool = new LeaseImpl[capacity];
    _state = new BitSet(capacity);
    indexes = new LinkedBlockingDeque<Integer>(capacity);
    for (int i = 0; i < capacity; i++) {
      indexes.add(i);
    }
  }

  //// METHODS

  @Override
  public Lease<T> lease() {
    Integer ind = indexes.pollFirst();
    if (ind == null) {
      return null;
    }
    return leaseInternal(ind);
  }

  @Override
  public Lease<T> lease(long timeout) throws InterruptedException {
    Integer ind = indexes.pollFirst(timeout, TimeUnit.MILLISECONDS);
    if (ind == null) {
      return null;
    }
    return leaseInternal(ind);
  }

  @Override
  public int available() {
    int size = 0;
    for (int i = 0; i < _pool.length; i++) {
      if (_state.get(i)) { // check that obj is free.
        size++;
      }
    }
    return size;
  }

  @Override
  public int capacity() {
    return capacity;
  }

  @SuppressWarnings("unchecked")
  private Lease<T> leaseInternal(int ind) {
    _state.set(ind, false); // set to 0 - busy.
    LeaseImpl lease = _pool[ind];
    if (lease == null) {
      _pool[ind] = (lease = new LeaseImpl(this, ind, lifecycle.build()));
    }
    return lease;
  }

  @Override
  public void destroy() {
    for (LeaseImpl lease : _pool) {
      lease.destroy();
    }
  }
}
