package org.zeromq.support.pool;

import org.zeromq.support.HasDestroy;

public interface Pool<T> extends HasDestroy {

  Lease<T> lease();

  Lease<T> lease(long timeout) throws InterruptedException;

  int available();

  int capacity();
}
