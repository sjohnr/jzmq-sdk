package org.zeromq.support.pool;

public interface Lease<T> {

  T get();

  void release();
}
