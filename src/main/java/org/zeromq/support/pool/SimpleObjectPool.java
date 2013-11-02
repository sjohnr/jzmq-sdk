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
import org.zeromq.support.ObjectBuilder;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

public final class SimpleObjectPool<T> implements ObjectPool<T> {

  public static final int DEFAULT_CAPACITY = 64; // just best guess.

  private static class LeaseImpl<T> implements Lease<T> {

    final ObjectPool<T> objectPool;
    final int oid;
    final T obj;

    LeaseImpl(ObjectPool<T> objectPool, int oid, T obj) {
      this.objectPool = objectPool;
      this.oid = oid;
      this.obj = obj;
    }

    public int oid() {
      return oid;
    }

    public T get() {
      return obj;
    }

    public void release() {
      objectPool.release(this);
    }
  }

  private final int capacity;
  private final ObjectBuilder<T> objectBuilder;

  private final AtomicReferenceArray<Lease<T>> _pool;
  private final BlockingDeque<Integer> _freeOids;

  //// CONSTRUCTORS

  public SimpleObjectPool(ObjectBuilder<T> objectBuilder) {
    this(DEFAULT_CAPACITY, objectBuilder);
  }

  public SimpleObjectPool(int capacity, ObjectBuilder<T> objectBuilder) {
    this.capacity = capacity;
    this.objectBuilder = objectBuilder;

    _pool = new AtomicReferenceArray<Lease<T>>(capacity);
    _freeOids = new LinkedBlockingDeque<Integer>(capacity);
    for (int oid = 0; oid < capacity; oid++) {
      _freeOids.add(oid);
    }
  }

  //// METHODS

  @Override
  public Lease<T> lease() {
    Integer oid = _freeOids.pollFirst();
    if (oid == null) {
      return null;
    }
    return _lease(oid);
  }

  @Override
  public Lease<T> lease(long timeout) {
    Integer oid;
    try {
      oid = _freeOids.pollFirst(timeout, TimeUnit.MILLISECONDS);
      if (oid == null) {
        return null;
      }
    }
    catch (InterruptedException e) {
      Thread.interrupted();
      throw ZmqException.seeCause(e);
    }
    return _lease(oid);
  }

  @Override
  public void release(Lease<T> lease) {
    boolean set = _pool.compareAndSet(lease.oid(), null, lease);
    if (set) {
      _freeOids.offerFirst(lease.oid());
    }
  }

  @Override
  public int size() {
    int size = 0;
    for (int i = 0; i < _pool.length(); i++) {
      if (_pool.get(i) != null) {
        size++;
      }
    }
    return size;
  }

  @Override
  public int capacity() {
    return capacity;
  }

  private Lease<T> _lease(Integer oid) {
    Lease<T> lease = _pool.getAndSet(oid, null);
    if (lease == null) {
      lease = new LeaseImpl<T>(this, oid, objectBuilder.build());
    }
    return lease;
  }
}
