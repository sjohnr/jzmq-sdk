package org.zeromq.support.pool;

public interface PoolObjectLifecycle<T> {

  T build();

  void destroy(T t);
}
