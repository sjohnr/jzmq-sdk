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

import org.zeromq.messaging.ZmqException;
import org.zeromq.support.HasDestroy;
import org.zeromq.support.ObjectBuilder;

import java.util.BitSet;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public final class SimpleObjectPool<T extends HasDestroy> implements ObjectPool<T> {

  public static final int DEFAULT_CAPACITY = 64; // just best guess.

  private static class LeaseImpl<T extends HasDestroy> implements Lease<T> {

    final SimpleObjectPool<T> objectPool;
    final int i;
    final T obj;

    LeaseImpl(SimpleObjectPool<T> objectPool, int i, T obj) {
      this.objectPool = objectPool;
      this.i = i;
      this.obj = obj;
    }

    public int i() {
      return i;
    }

    public T get() {
      return obj;
    }

    public void release() {
      objectPool.release(this);
    }

    @Override
    public void destroy() {
      obj.destroy();
    }
  }

  private final int capacity;
  private final ObjectBuilder<T> objectBuilder;

  private final Lease<T>[] _pool;
  private final BlockingDeque<Integer> indexes;
  private final BitSet _poolState; // 1 - obj in pool is free, 0 - obj in pool is busy.

  //// CONSTRUCTORS

  public SimpleObjectPool(ObjectBuilder<T> objectBuilder) {
    this(DEFAULT_CAPACITY, objectBuilder);
  }

  @SuppressWarnings("unchecked")
  public SimpleObjectPool(int capacity, ObjectBuilder<T> objectBuilder) {
    this.capacity = capacity;
    this.objectBuilder = objectBuilder;

    _pool = new Lease[capacity];
    _poolState = new BitSet(capacity);
    indexes = new LinkedBlockingDeque<Integer>(capacity);
    for (int i = 0; i < capacity; i++) {
      indexes.add(i);
    }
  }

  //// METHODS

  @Override
  public Lease<T> lease() {
    Integer i = indexes.pollFirst();
    if (i == null) {
      return null;
    }
    return _lease(i);
  }

  @Override
  public Lease<T> lease(long timeout) {
    Integer i;
    try {
      i = indexes.pollFirst(timeout, TimeUnit.MILLISECONDS);
      if (i == null) {
        return null;
      }
    }
    catch (InterruptedException e) {
      Thread.interrupted();
      throw ZmqException.seeCause(e);
    }
    return _lease(i);
  }

  @Override
  public int available() {
    int size = 0;
    for (int i = 0; i < _pool.length; i++) {
      if (_poolState.get(i)) { // check that obj is free.
        size++;
      }
    }
    return size;
  }

  @Override
  public int capacity() {
    return capacity;
  }

  private Lease<T> _lease(int i) {
    _poolState.set(i, false); // set to 0 - busy.
    Lease<T> lease = _pool[i];
    if (lease == null) {
      _pool[i] = (lease = new LeaseImpl<T>(this, i, objectBuilder.build()));
    }
    return lease;
  }

  @Override
  public void destroy() {
    for (Lease<T> lease : _pool) {
      lease.destroy();
    }
  }

  void release(Lease<T> lease) {
    _poolState.set(lease.i(), true); // set to 1 - free.
    indexes.offerFirst(lease.i());
  }
}
